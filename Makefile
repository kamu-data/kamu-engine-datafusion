TARGET_ARCH=x86_64-unknown-linux-musl
ENGINE_VERSION = 0.1.0
ENGINE_IMAGE_TAG = $(ENGINE_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-datafusion:$(ENGINE_IMAGE_TAG)


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
