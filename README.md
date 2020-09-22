# RADAR pRMT iOS

RADAR-base pRMT app for iOS 11.0 or higher. The code is completely written in Swift. The app is work in progress, and only records location data without sending it to a server. In contrast with RADAR pRMT Android, this app is not (yet) modular: all code and plugins are bundled in this single repository. This may change in the future.

# Development

Use XCode to modify the code. Install dependencies with [Carthage](https://github.com/Carthage/Carthage):

```shell
export XCODE_XCCONFIG_FILE="$(pwd)/carthage-fix.xcconfig"
carthage update --platform iOS --no-use-binaries --cache-builds
rm -rf Frameworks
mkdir Frameworks
cp -r Carthage/Build/iOS/*.framework* Frameworks
```

# Contributing

Make pull requests to contribute to this code base.
