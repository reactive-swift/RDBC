//===--- RDBC.swift ------------------------------------------------------===//
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

import Foundation
import Boilerplate
import ExecutionContext
import Future

public protocol DialectRich {
    var dialect:Dialect {get}
}

public protocol Dialect {
    var proto:String {get}
}

public protocol ConnectionFactory {
    func connect(url:String, params:Dictionary<String, String>) -> Future<Connection>
}

public protocol PoolFactory {
    func pool(url:String, params:Dictionary<String, String>) throws -> ConnectionPool
}

public extension ConnectionFactory {
    func connect(url:String) -> Future<Connection> {
        return connect(url: url, params: [:])
    }
}

public extension PoolFactory {
    func pool(url:String) throws -> ConnectionPool {
        return try pool(url: url, params: [:])
    }
}

public extension ResultSet {
    private func accumulate(rows:[Row]) -> Future<[Row]> {
        return self.next().map { row in
            (rows, row.map {rows + [$0]})
        }.flatMap { (old, new) -> Future<[Row]> in
            if let new = new {
                return self.accumulate(rows: new)
            } else {
                return Future<[Row]>(value: old)
            }
        }
    }
    
    public func all() -> Future<[Row]> {
        return accumulate(rows: [Row]())
    }
}

public class ConnectionPool : Connection {
    private let _connectionFactory:()->Future<Connection>
    
    public init(connectionFactory:@escaping ()->Future<Connection>) {
        _connectionFactory = connectionFactory
    }
    
    public func execute(query: String, parameters: [Any?], named: [String : Any?]) -> Future<ResultSet?> {
        return _connectionFactory().flatMap { connection in
            connection.execute(query: query, parameters: parameters, named: named)
        }
    }
}

class DialectRichConnectionPool : ConnectionPool, DialectRich {
    let dialect: Dialect
    
    init(dialect:Dialect, connectionFactory:@escaping ()->Future<Connection>) {
        self.dialect = dialect
        super.init(connectionFactory: connectionFactory)
    }
}

extension ConnectionPool {
    static func pool(dialect:Dialect?, connectionFactory:@escaping ()->Future<Connection>) -> ConnectionPool {
        return dialect.map { dialect in
            DialectRichConnectionPool(dialect: dialect, connectionFactory: connectionFactory)
        } ?? ConnectionPool(connectionFactory: connectionFactory)
    }
}

public class RDBC : ConnectionFactory, PoolFactory {
    private var _drivers = [String:(Driver, Dialect?)]()
    private let _contextFactory:()->ExecutionContextProtocol
    
    public init() {
        _contextFactory = {ExecutionContext(kind: .serial)}
    }
    
    public func register(driver: Driver, dialect: Dialect? = nil) {
        _drivers[driver.proto] = (driver, dialect)
    }
    
    private func async(driver: SyncDriver, dialect:Dialect? = nil) -> (Driver, Dialect?) {
        let dialect = dialect.or(else: (driver as? DialectRich)?.dialect)
        let driver = AsyncDriver(driver: driver, contextFactory: _contextFactory)
        
        return dialect.map { dialect -> (Driver, Dialect?) in
            (driver, dialect)
        } ?? (driver, nil)
    }
    
    public func register(driver: SyncDriver, dialect: Dialect? = nil) {
        async(driver: driver, dialect: dialect) |> register
    }
    
    public func pool(url:String, params:Dictionary<String, String>) throws -> ConnectionPool {
        let (_, dialect) = try self.driver(url: url, params: params)
        
        return ConnectionPool.pool(dialect: dialect) {
            self.connect(url: url, params: params)
        }
    }
    
    public func driver(url _url: String, params: Dictionary<String, String>) throws -> (Driver, Dialect?) {
        guard let url = URL(string: _url) else {
            throw RDBCFrameworkError.invalid(url: _url)
        }
        
        guard let proto = url.scheme else {
            throw RDBCFrameworkError.noProtocol
        }
        
        guard let driver = _drivers[proto] else {
            throw RDBCFrameworkError.unknown(protocol: proto)
        }
        
        return driver
    }
    
    public func connect(url: String, params: Dictionary<String, String>) -> Future<Connection> {
        return future(context: immediate) {
            try self.driver(url: url, params: params)
        }.flatMap { (driver, _) in
            //TODO: wrap connnection
            driver.connect(url: url, params: params)
        }
    }
}
