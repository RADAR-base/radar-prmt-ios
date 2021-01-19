// swift-tools-version:5.3

import PackageDescription

let package = Package(
  name: "radar-prmt-ios",
  defaultLocalization: "en",
  platforms: [
    .iOS(.v12),
  ],
  dependencies: [
    .package(url: "https://github.com/thehyve/BlueSteel.git", from: "4.0.0"), 
    .package(url: "https://github.com/httpswift/swifter.git", from: "1.5.0"), 
    .package(url: "https://github.com/ReactiveX/RxSwift.git", from: "5.1.0"), 
    .package(url: "https://github.com/RxSwiftCommunity/RxSwiftExt.git", from: "5.2.0"), 
    .package(name: "Gzip", url: "https://github.com/1024jp/GzipSwift.git", from: "5.0.0"), 
    .package(url: "https://github.com/square/Valet.git", from: "3.2.3"), 
    .package(name: "JWTDecode", url: "https://github.com/auth0/JWTDecode.swift.git", from: "2.5.0"), 
    .package(url: "https://github.com/pixeldock/RxAppState.git", from: "1.5.0"), 
   // .package(name: "Firebase", url: "https://github.com/firebase/firebase-ios-sdk.git", from: "7.4.0"), 
  ],
  targets: [
    .target(
        name: "radar-prmt-ios",
        dependencies: [
          "Valet",
          "BlueSteel",
          "JWTDecode",
          "RxSwift",
          "RxSwiftExt",
          "Gzip",
          "RxAppState",
          //.product(name: "FirebaseCrashlytics", package: "Firebase"),
          //.product(name: "FirebaseAnalytics", package: "Firebase"),
          //.product(name: "FirebaseRemoteConfig", package: "Firebase"),
        ],
        exclude: ["Info.plist"],
        resources: [
          .process("GoogleService-Info.plist"),
          .process("config.plist"),
          .process("radar-schemas.bundle"),
          .process("QR_code_for_mobile_English_Wikipedia.svg.png"),
        ]),
    .testTarget(
        name: "radar-prmt-iosTests",
        dependencies: ["radar-prmt-ios"],
        exclude: ["Info.plist"]),
  ]
)
