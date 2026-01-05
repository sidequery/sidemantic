FROM ubuntu:22.04

RUN apt-get update -qq && \
    apt-get install -qq -y curl unzip git && \
    rm -rf /var/lib/apt/lists/*

# Install latest Rill
RUN ARCH=$(uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/') && \
    VERSION=$(curl -sL https://cdn.rilldata.com/rill/latest.txt) && \
    curl -sL https://cdn.rilldata.com/rill/${VERSION}/rill_linux_${ARCH}.zip -o /tmp/rill.zip && \
    unzip -q /tmp/rill.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/rill && \
    rm /tmp/rill.zip

WORKDIR /project

EXPOSE 9009

CMD ["rill", "start", "--no-open"]
