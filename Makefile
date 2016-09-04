.PHONY: all compile doc test clean

all: compile doc

compile:
	$(MAKE) -C erlang $@

doc:
	$(MAKE) -C erlang html

test:
	$(MAKE) -C erlang eunit

clean:
	$(MAKE) -C erlang $@
	rm -f debian/erlang-tcpcall.install
