TESTS = go-erl go-erl-parallel

.PHONY: all clean $(TESTS)

all: $(TESTS)

clean: $(TESTS)

$(TESTS):
	@echo "\n*** TEST $@ ***\n"
	$(MAKE) -C $@ $(MAKECMDGOALS)
