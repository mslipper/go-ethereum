package pkg

import (
	log "github.com/inconshreveable/log15"
	"fmt"
	"time"
	"strings"
	"regexp"
	"math"
	"strconv"
)

func NewLogger(module string) log.Logger {
	return log.New("module", module)
}

type PrettyDuration time.Duration

var prettyDurationRe = regexp.MustCompile(`\.[0-9]+`)

func (d PrettyDuration) String() string {
	label := fmt.Sprintf("%v", time.Duration(d))
	if match := prettyDurationRe.FindString(label); len(match) > 4 {
		label = strings.Replace(label, match, match[:4], 1)
	}
	return label
}

type PrettySize int

var sizes = []string{
	"",
	"KB",
	"MB",
	"GB",
	"TB",
	"PB",
	"EB",
	"ZB",
	"YB",
}

func (d PrettySize) String() string {
	if d == 0 {
		return "0"
	}

	k := 1024
	i := int(math.Floor(math.Log(float64(d)) / math.Log(float64(k))))
	return strconv.FormatFloat(float64(d) / math.Pow(float64(k), float64(i)), 'f', 2, 64) + sizes[i]
}