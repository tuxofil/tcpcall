.PHONY: all clean

ifndef ($(HOME))
    export HOME = /tmp
endif

all:
	go run test.go

clean:
	rm -f test erl_crash.dump test_flymake *~
