import XCTest

import RDBCTests

var tests = [XCTestCaseEntry]()

tests += RDBCTests.allTests()

XCTMain(tests)
