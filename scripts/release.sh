#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
version=${1:?Usage: bash scripts/release.sh vMAJOR.MINOR.PATCH}
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "Invalid version: $version" >&2
  exit 1
fi

package_version=$(node -p "require('./package.json').version")
if [[ "$version" != "v$package_version" ]]; then
  echo "Tag $version must match package.json version $package_version" >&2
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
npm_package="$stage/npm-package"
mkdir -p "$npm_package/npm/native"
cp package.json README.md LICENSE "$npm_package/"
cp npm/*.cjs "$npm_package/npm/"

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
        -trimpath -ldflags="-s -w -X github.com/eyotang/s3flow/internal/update.Version=$version" -o "$package/$command$suffix" "./cmd/$command"
    done
    node_os="$os"
    node_arch="$arch"
    if [[ "$os" == windows ]]; then node_os=win32; fi
    if [[ "$arch" == amd64 ]]; then node_arch=x64; fi
    native="$npm_package/npm/native/$node_os-$node_arch"
    mkdir -p "$native"
    cp "$package/s3get$suffix" "$package/s3push$suffix" "$native/"
    cp README.md LICENSE "$package/"
    if [[ "$os" == windows ]]; then
      (cd "$stage" && zip -qr "$output/$name.zip" "$name")
    else
      tar -czf "$output/$name.tar.gz" -C "$stage" "$name"
    fi
  done
done

(cd "$npm_package" && npm pack --ignore-scripts --cache "$stage/npm-cache" --pack-destination "$output")
(cd "$output" && shasum -a 256 ./*.tar.gz ./*.zip ./*.tgz > checksums.txt)
echo "Release artifacts: $output"
