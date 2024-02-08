TARGET_ARCH = x86_64-unknown-linux-musl
PLATFORM = linux/amd64
CRATE_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu-engine-datafusion") | .version')
IMAGE_TAG = $(CRATE_VERSION)
IMAGE = ghcr.io/kamu-data/engine-datafusion:$(IMAGE_TAG)


###############################################################################
# Lint
###############################################################################

.PHONY: lint
lint:
	cargo fmt --check
	cargo deny check


###############################################################################
# Test
###############################################################################

.PHONY: test
test:
	RUST_LOG_SPAN_EVENTS=new,close RUST_LOG=debug cargo nextest run


###############################################################################
# Build
###############################################################################

.PHONY: build
build:
	RUSTFLAGS="" cross build --release --target $(TARGET_ARCH)


.PHONY: image
image:
	docker buildx build \
	    --platform $(PLATFORM) \
		--build-arg target_arch=$(TARGET_ARCH) \
		-t $(IMAGE) \
		-f image/Dockerfile \
		--load \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE)


###############################################################################
# Release
###############################################################################

.PHONY: release-patch
release-patch:
	cargo set-version --workspace --bump patch

.PHONY: release-minor
release-minor:
	cargo set-version --workspace --bump minor

.PHONY: release-major
release-major:
	cargo set-version --workspace --bump major
