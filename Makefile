.PHONY: all compile doc test fmt clean

all: compile doc

compile:
	$(MAKE) -C erlang $@

doc:
	$(MAKE) -C erlang html

test:
	$(MAKE) -C erlang eunit
	$(MAKE) -C golang $@

fmt:
	find . -type f -name \*.go -exec gofmt -w '{}' ';'

clean:
	$(MAKE) -C erlang $@
	$(MAKE) -C golang $@
	rm -f debian/erlang-tcpcall.install
