import XCTest

#if os(Linux)
public func allTests() -> [XCTestCaseEntry] {
	 return [
		testCase(PoolTests.allTests)
	]
}
#endif