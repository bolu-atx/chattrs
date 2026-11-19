// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "chattrs-tray",
    platforms: [.macOS(.v13)],
    targets: [
        .target(name: "TrayCore"),
        .executableTarget(name: "chattrs-tray", dependencies: ["TrayCore"]),
        .testTarget(name: "TrayCoreTests", dependencies: ["TrayCore"]),
    ]
)
