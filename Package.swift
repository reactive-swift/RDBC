import PackageDescription

let package = Package(
    name: "RDBC",
    dependencies: [
        .Package(url: "https://github.com/reactive-swift/Future.git", majorVersion: 0, minor: 2),
        //.Package(url: "https://github.com/IBM-Swift/CLibpq.git", majorVersion: 0, minor: 1),
    ]
)
