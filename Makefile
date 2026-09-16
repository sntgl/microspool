# microspool — build
#
#   make native    host build (cc) for tests
#   make arm       static ARMv7 hard-float binary (zig cc + musl) — e.g. FlashForge AD5M
#   make release   static binaries for armv7, aarch64, x86_64 (zig cc + musl) into build/
#   make test      run test_microspool.py against the native build (tornado via uv)
#   make size      size/format of the release binaries
#   make clean

VERSION     ?= 0.2.0
CC_NATIVE   ?= cc
PYTHON3     ?= python3
CFLAGS      := -std=c11 -Wall -Wextra -O2 -Ivendor -I. -DMICROSPOOL_VERSION=\"$(VERSION)\"
LDFLAGS_NATIVE := -lm

ZIG         ?= zig
CROSS_CFLAGS  := -std=c11 -Wall -Wextra -Os -Ivendor -I. -DMICROSPOOL_VERSION=\"$(VERSION)\" \
                 -ffunction-sections -fdata-sections
CROSS_LDFLAGS := -static -s -Wl,--gc-sections -lm

BUILD_DIR   := build
NATIVE_BIN  := microspool
SRCS        := microspool.c vendor/cJSON.c
HDRS        := vendor/cJSON.h

UI_HTML     := ui/index.html
UI_HDR      := $(BUILD_DIR)/ui_gz.h

TARGETS     := armv7:arm-linux-musleabihf aarch64:aarch64-linux-musl x86_64:x86_64-linux-musl
RELEASE_BINS := $(foreach t,$(TARGETS),$(BUILD_DIR)/microspool-$(firstword $(subst :, ,$(t))))

.PHONY: all native arm release test size clean

all: native

native: $(NATIVE_BIN)

$(NATIVE_BIN): $(SRCS) $(HDRS) $(UI_HDR)
	$(CC_NATIVE) $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS_NATIVE)

$(UI_HDR): $(UI_HTML) tools/embed_ui.py
	@mkdir -p $(BUILD_DIR)
	$(PYTHON3) tools/embed_ui.py $(UI_HTML) $(UI_HDR)

arm: $(BUILD_DIR)/microspool-armv7

release: $(RELEASE_BINS)

define CROSS_RULE
$(BUILD_DIR)/microspool-$(1): $(SRCS) $(HDRS) $(UI_HDR)
	mkdir -p $(BUILD_DIR)
	$(ZIG) cc $(CROSS_CFLAGS) -target $(2) -o $$@ $(SRCS) $(CROSS_LDFLAGS)
endef
$(foreach t,$(TARGETS),$(eval $(call CROSS_RULE,$(firstword $(subst :, ,$(t))),$(lastword $(subst :, ,$(t))))))

size: release
	@ls -l $(RELEASE_BINS)
	@file $(RELEASE_BINS)

test: native
	@# warm-up: macOS may delay the first exec of a fresh binary (Gatekeeper/EDR)
	@./$(NATIVE_BIN) -V >/dev/null 2>&1; true
	uv run --with tornado python3 test_microspool.py

clean:
	rm -f $(NATIVE_BIN)
	rm -rf $(BUILD_DIR)
	rm -f *.o
