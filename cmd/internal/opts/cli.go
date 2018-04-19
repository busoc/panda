package opts

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type SIDSet []uint32

func (i *SIDSet) Set(vs string) error {
	if f, err := os.Open(vs); err == nil {
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			t := s.Text()
			if t == "" {
				continue
			}
			if err := i.parse(t); err != nil {
				return err
			}
		}
		return s.Err()
	}
	for _, v := range strings.Split(vs, ",") {
		if err := i.parse(v); err != nil {
			return err
		}
	}
	return nil
}

func (i *SIDSet) parse(v string) error {
	n, err := strconv.ParseUint(v, 0, 64)
	if err != nil {
		return err
	}
	*i = append(*i, uint32(n))
	return nil
}

func (i *SIDSet) String() string {
	return fmt.Sprint(*i)
}

type UMISet []uint64

func (i *UMISet) Set(vs string) error {
	if f, err := os.Open(vs); err == nil {
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			t := s.Text()
			if t == "" {
				continue
			}
			if err := i.parse(t); err != nil {
				return err
			}
		}
		return s.Err()
	}
	for _, v := range strings.Split(vs, ",") {
		if err := i.parse(v); err != nil {
			return err
		}
	}
	return nil
}

func (i *UMISet) String() string {
	return fmt.Sprint(*i)
}

func (i *UMISet) parse(v string) error {
	n, err := strconv.ParseUint(v, 0, 64)
	if err != nil {
		return err
	}
	*i = append(*i, uint64(n))
	return nil
}
