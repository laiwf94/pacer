# pacer build. `make` builds release binaries, `make check` runs the gate.
# Variants live in build/<variant>: release, asan (ASan+UBSan), tsan, cov.

CC       := clang
VARIANT  ?= release
BUILD    := build/$(VARIANT)
UNAME    := $(shell uname -s)
JOBS     ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu)

CPPFLAGS := -Iinclude -Isrc -D_GNU_SOURCE
WARN     := -Wall -Wextra -Werror -Wpedantic -Wshadow -Wconversion -Wstrict-prototypes \
            -Wmissing-prototypes -Wformat=2 -Wundef -Wcast-qual -Wvla
CFLAGS   := -std=c17 $(WARN) -g -pthread -MMD -MP
LDFLAGS  := -pthread

ifeq ($(VARIANT),release)
  CFLAGS += -O2 -D_FORTIFY_SOURCE=2 -fstack-protector-strong
else ifeq ($(VARIANT),asan)
  SAN := -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
  CFLAGS += -O1 $(SAN)
  LDFLAGS += $(SAN)
else ifeq ($(VARIANT),tsan)
  CFLAGS += -O1 -fsanitize=thread
  LDFLAGS += -fsanitize=thread
else ifeq ($(VARIANT),cov)
  CFLAGS += -O0 -fprofile-instr-generate -fcoverage-mapping
  LDFLAGS += -fprofile-instr-generate
else
  $(error unknown VARIANT $(VARIANT))
endif

LIB_SRC   := $(wildcard src/lib/*.c)
TEST_SRC  := $(wildcard tests/test_*.c) tests/test_main.c
LIB_OBJ   := $(LIB_SRC:%.c=$(BUILD)/%.o)
TEST_OBJ  := $(sort $(TEST_SRC:%.c=$(BUILD)/%.o))

ALL_C     := $(LIB_SRC) $(sort $(TEST_SRC))
ALL_SRC   := $(ALL_C) $(wildcard include/*.h src/*/*.h tests/*.h)

.PHONY: all lib test run-tests check format format-check tidy coverage clean require-linux

all: lib

lib: $(BUILD)/libpacer.a

$(BUILD)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(BUILD)/libpacer.a: $(LIB_OBJ)
	@rm -f $@
	ar rcs $@ $^

$(BUILD)/unit: $(TEST_OBJ) $(BUILD)/libpacer.a
	$(CC) $(LDFLAGS) -o $@ $^

run-tests: $(BUILD)/unit
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
	UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
	TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
	LLVM_PROFILE_FILE=$(abspath $(BUILD))/prof/%p-%m.profraw \
	$(BUILD)/unit

test:
	$(MAKE) VARIANT=asan run-tests

format:
	clang-format -i $(ALL_SRC)

format-check:
	clang-format --dry-run --Werror $(ALL_SRC)

tidy:
	printf '%s\n' $(ALL_C) | xargs -P $(JOBS) -I{} clang-tidy --quiet {} -- $(CPPFLAGS) -std=c17

coverage:
	rm -rf build/cov/prof
	$(MAKE) VARIANT=cov run-tests
	scripts/coverage.sh build/cov 90 build/cov/unit

require-linux:
ifneq ($(UNAME),Linux)
	@echo "make check needs Linux (epoll, signalfd, sanitizers). Run: scripts/dev.sh make check" >&2
	@exit 2
endif

check: require-linux
	$(MAKE) format-check
	$(MAKE) tidy
	$(MAKE) -j$(JOBS) VARIANT=release all
	$(MAKE) -j$(JOBS) VARIANT=asan run-tests
	$(MAKE) -j$(JOBS) VARIANT=tsan run-tests
	$(MAKE) -j$(JOBS) coverage
	@echo "check: all gates passed"

clean:
	rm -rf build

-include $(LIB_OBJ:.o=.d) $(TEST_OBJ:.o=.d)
