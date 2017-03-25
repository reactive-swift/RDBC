import PackageDescription

let package = Package(
    name: "RDBC",
    targets: [
        Target(
            name: "RDBC"
        ),
    ],
    dependencies: [
        .Package(url: "https://github.com/reactive-swift/Future.git", "0.2.0-alpha"),
        //.Package(url: "https://github.com/IBM-Swift/CLibpq.git", majorVersion: 0, minor: 1),
    ]
)
