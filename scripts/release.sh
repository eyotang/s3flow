#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
version=${1:?Usage: bash scripts/release.sh vMAJOR.MINOR.PATCH}
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "Invalid version: $version" >&2
  exit 1
fi

output="$(pwd)/dist/$version"
if [[ -e "$output" ]]; then
  echo "Output already exists: $output (remove it before rebuilding)" >&2
  exit 1
fi
mkdir -p "$output"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

for os in linux darwin windows; do
  for arch in amd64 arm64; do
    name="s3flow_${version}_${os}_${arch}"
    package="$stage/$name"
    mkdir -p "$package"
    suffix=""
    if [[ "$os" == windows ]]; then suffix=.exe; fi
    for command in s3get s3push; do
      echo "Building $command for $os/$arch"
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build \
        -trimpath -ldflags='-s -w' -o "$package/$command$suffix" "./cmd/$command"
    done
    cp README.md LICENSE "$package/"
    if [[ "$os" == windows ]]; then
      (cd "$stage" && zip -qr "$output/$name.zip" "$name")
    else
      tar -czf "$output/$name.tar.gz" -C "$stage" "$name"
    fi
  done
done

(cd "$output" && shasum -a 256 ./*.tar.gz ./*.zip > checksums.txt)
echo "Release artifacts: $output"
