TARGET_ARCH = x86_64-unknown-linux-musl
ENGINE_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu-engine-datafusion") | .version')
ENGINE_IMAGE_TAG = $(ENGINE_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-datafusion:$(ENGINE_IMAGE_TAG)


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

# Do not use except for local testing - release images are built via CI
.PHONY: build
build:
	RUSTFLAGS="" cross build --release --target $(TARGET_ARCH)


.PHONY: image
image: build
	docker build \
		--build-arg target_arch=$(TARGET_ARCH) \
		--build-arg version=$(ENGINE_VERSION) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)


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
