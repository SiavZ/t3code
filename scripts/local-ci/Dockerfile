# The local CI runner image: Ubuntu 24.04 like GitHub's ubuntu-24.04 runners,
# with what .github/workflows/ci.yml installs. Tests depend on that distro's
# tools (jq 1.7 fails on empty input, 1.6 does not), so keep the base in step.
FROM ubuntu:24.04

ARG NODE_VERSION=26.12.1
# Keep in step with the playwright version in apps/web and apps/server.
ARG PLAYWRIGHT_VERSION=1.60.0

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl dbus git jq libsecret-1-dev openssh-client pkg-config \
    python3 libwayland-dev libxkbcommon-dev xz-utils \
  && arch=$(dpkg --print-architecture | sed 's/amd64/x64/') \
  && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${arch}.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1 \
  && PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    npx --yes "playwright@${PLAYWRIGHT_VERSION}" install --with-deps --only-shell chromium \
  && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --component rustfmt

# GitHub runners test as an unprivileged user; as root, permission tests cannot fail a read.
RUN useradd --create-home --uid 1001 runner \
  && mkdir -p /work /pnpm /pnpm-store /usr/local/cargo/registry \
  && chown -R runner /work /pnpm /pnpm-store /usr/local/cargo
USER runner

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright PNPM_HOME=/pnpm \
  PATH=/pnpm/bin:/pnpm:/work/node_modules/.bin:$PATH
WORKDIR /work
