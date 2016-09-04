TESTS = go-erl

.PHONY: all clean $(TESTS)

all: $(TESTS)

clean: $(TESTS)

$(TESTS):
	@echo "\n*** TEST $@ ***\n"
	$(MAKE) -C $@ $(MAKECMDGOALS)
