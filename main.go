package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type queue struct {
	msgs    []string
	waiters []chan string
}

var (
	mu     sync.Mutex
	queues = map[string]*queue{}
)

// getQ возвращает очередь по имени; создаёт новую, если не существует.
func getQ(name string) *queue {
	q, ok := queues[name]
	if !ok {
		q = &queue{}
		queues[name] = q
	}

	return q
}

// putMsg кладёт сообщение в очередь. Если есть ждущий получатель — отдаёт сообщение ему напрямую.
func putMsg(name, msg string) {
	mu.Lock()
	defer mu.Unlock()

	q := getQ(name)
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		ch <- msg

		return
	}

	q.msgs = append(q.msgs, msg)
}

// fetchMsg забирает первое сообщение из очереди по принципу FIFO.
// При пустой очереди ждёт появления сообщения до timeout секунд.
func fetchMsg(name string, timeout int) (string, bool) {
	mu.Lock()

	q := getQ(name)
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		mu.Unlock()

		return msg, true
	}

	if timeout == 0 {
		mu.Unlock()

		return "", false
	}

	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	mu.Unlock()

	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(time.Duration(timeout) * time.Second):
		mu.Lock()

		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)

				break
			}
		}

		mu.Unlock()

		// Защита от гонки между таймаутом и PUT.
		// PUT мог отправить в ch между таймаутом и удалением из waiters.
		select {
		case msg := <-ch:
			return msg, true
		default:
			return "", false
		}
	}
}

// handler обрабатывает HTTP-запросы: PUT кладёт сообщение в очередь, GET забирает.
func handler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	switch r.Method {
	case http.MethodPut:
		msg := r.URL.Query().Get("v")
		if msg == "" {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		putMsg(name, msg)

	case http.MethodGet:
		timeout := 0
		ts := r.URL.Query().Get("timeout")

		if ts != "" {
			v, err := strconv.Atoi(ts)
			if err == nil {
				timeout = v
			}
		}

		msg, ok := fetchMsg(name, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		_, _ = w.Write([]byte(msg)) //nolint:gosec

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}

	http.HandleFunc("/", handler)

	srv := &http.Server{
		Addr:              ":" + os.Args[1],
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	err := srv.Shutdown(shutdownCtx)
	cancel()

	if err != nil {
		os.Exit(1)
	}
}
