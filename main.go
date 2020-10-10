package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jasonlvhit/gocron"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type writerFunc func(p []byte) (n int, err error)

func (wf writerFunc) Write(p []byte) (n int, err error) { return wf(p) }

type CancelContainer struct {
	cancel context.CancelFunc
}

func main() {
	sourceAddr := flag.String("source", "127.0.0.1:30003", "source address host:port")
	csvPrefix := flag.String("prefix", "adsb", "prefix of csv file")
	splitAt := flag.String("split-at", "00:00", "time to split file")
	flag.Parse()

	cc := CancelContainer{
		cancel: func() {
			log.Print("nothing to cancel yet")
		},
	}

	err := gocron.Every(1).Day().At(*splitAt).Do(func() {
		log.Print("Time to rotate csv file.")
		cc.cancel()
	})
	if err != nil {
		log.Fatalf("Failed to schedule log rotation. err=[%v]", err)
	}
	go recordLoop(&cc, *sourceAddr, *csvPrefix, *splitAt)
	<-gocron.Start()
}

func recordLoop(cc *CancelContainer, sourceAddr string, csvPrefix string, splitAt string) {
	for {
		ctx, cancel := context.WithCancel(context.Background())
		cc.cancel = cancel
		err := record(ctx, sourceAddr, csvPrefix, splitAt)
		if err != nil {
			log.Printf("record got error=[%v]", err)
			time.Sleep(1 * time.Second)
		}
		log.Println("Restarting")
	}
}

func record(ctx context.Context, sourceAddr string, csvPrefix string, splitAt string) error {
	sourceTcp, err := net.Dial("tcp", sourceAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %v", err)
	}
	defer sourceTcp.Close()

	csvWriter, err := openCsvFile(csvPrefix, splitAt)
	if err != nil {
		return err
	}
	defer csvWriter.Close()

	writer := writerFunc(func(p []byte) (n int, err error) {
		select {
		case <-ctx.Done():
			log.Println("Writer is about to stop (log rotation required).")
			return 0, ctx.Err()
		default:
			return csvWriter.Write(p)
		}
	})

	wc, err := io.Copy(writer, sourceTcp)
	if err != nil {
		return fmt.Errorf("failed to copy. error=[%v]", err)
	}
	log.Printf("EOF received (%d bytes are transfered).", wc)

	return nil
}

func openCsvFile(csvPrefix string, splitAt string) (*os.File, error) {
	now := time.Now()

	fn, err := csvFileName(csvPrefix, splitAt, now)
	if err != nil {
		return nil, err
	}
	log.Printf("file name = %s", fn)

	csvWriter, err := os.OpenFile(fn, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for writing: %v", err)
	}

	return csvWriter, nil
}

func csvFileName(csvPrefix string, splitAt string, now time.Time) (string, error) {
	hhmm := strings.Split(splitAt, ":")
	hh, err := strconv.ParseInt(hhmm[0], 10, 32)
	if err != nil {
		return "", err
	}

	mm, err := strconv.ParseInt(hhmm[1], 10, 32)
	if err != nil {
		return "", err
	}

	splitDate := time.Date(now.Year(), now.Month(), now.Day(), int(hh), int(mm), 0, 0, now.Location())

	if now.Before(splitDate) {
		splitDate = splitDate.AddDate(0, 0, -1)
	}

	fn := fmt.Sprintf("%s-%s.csv", csvPrefix, splitDate.Format("2006-01-02T1504"))
	return fn, nil
}
