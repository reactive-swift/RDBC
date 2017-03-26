//===--- PoolTests.swift ------------------------------------------------------===//
//Copyright (c) 2017 Crossroad Labs s.r.o.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//===----------------------------------------------------------------------===//

import XCTest

import RDBC

extension SyncResultSet {
    func at<T>(index: Int, as: T.Type) throws -> T {
        return try next()![index]! as! T
    }
    
    func at<T>(index: Int) throws -> T {
        return try at(index: index, as: T.self)
    }
    
    func count() throws -> Int {
        return try at(index: 0, as: Int.self)
    }
}

class PoolTests: XCTestCase {
    lazy var pool: Connection = try! {
        let rdbc = RDBC()
        rdbc.register(driver: MSDriver())
        return try rdbc.pool(url: "ms://memory")
    }()
    
    
}
