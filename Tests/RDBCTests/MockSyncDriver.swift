//===--- MockSyncDriver.swift ------------------------------------------------------===//
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

import RDBC

class MSResultSet: SyncResultSet {
    static let table: ([String], [[Any?]]) = (
        ["id", "firstname", "lastname"],
        [
            [1, "John", "Lennon"],
            [1, "Paul", "McCartney"]
        ]
    )
    
    private var _pos: Int = -1
    private var _cols: [String]
    private let _data: [[Any?]]
    
    init(table: ([String], [[Any?]])) {
        _cols = table.0
        _data = table.1
    }
    
    convenience init(count: Int) {
        self.init(table: (["count"], [[1]]))
    }
    
    convenience init() {
        self.init(table: MSResultSet.table)
    }
    
    func columnCount() throws -> Int {
        return _cols.count
    }
    
    func columns() throws -> [String] {
        return _cols
    }
    
    func reset() throws {
        _pos = -1
    }
    
    func next() throws -> [Any?]? {
        _pos = _pos.advanced(by: 1)
        return _pos < _data.count ? _data[_pos] : nil
    }
}

class MSConnection: SyncConnection {
    @discardableResult
    public func execute(query: String, parameters: [Any?], named: [String : Any?]) throws -> SyncResultSet? {
        return query.hasPrefix("SELECT") ? MSResultSet() : MSResultSet(count: 1)
    }

}

class MSDriver: SyncDriver {
    let proto: String = "ms"
    
    let poolSizeLimit: UInt = 1
    var poolSizeRecommended: UInt = 1
    
    func connect(url:String, params:Dictionary<String, String>) throws -> SyncConnection {
        return MSConnection()
    }
}
