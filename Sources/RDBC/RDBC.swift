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

private let _reserve: UInt = 1024

public class ResourcePool<Res> : Sequence, ExecutionContextTenantProtocol {
    public typealias Iterator = AnyIterator<Future<Res>>
    
    public let context: ExecutionContextProtocol
    
    private var _limit: UInt
    private let _factory: ()->Future<Res>
    
    private var _cache: [Res]
    private var _queue: [Promise<Res>]
    
    public init(context: ExecutionContextProtocol, limit: UInt = UInt.max, factory: @escaping ()->Future<Res>) {
        self.context = context //.serial
        self._limit = limit
        self._factory = factory
        
        self._cache = Array()
        self._queue = Array()
        
        _cache.reserveCapacity(Int([limit, _reserve].min()!))
        _queue.reserveCapacity(Int([limit * 2, _reserve].min()!))
    }
    
    public func reclaim(resource: Res) {
        context.async {
            guard !self._queue.isEmpty, let waiting = Optional(self._queue.removeFirst()) else {
                self._cache.append(resource)
                return
            }
            
            try waiting.success(value: resource)
        }
    }
    
    public func makeIterator() -> Iterator {
        let context = self.context
        return Iterator {
            future(context: context) { () -> Future<Res> in
                if !self._cache.isEmpty, let cached = Optional(self._cache.removeFirst()) {
                    return Future<Res>(value: cached)
                }
                
                guard self._limit <= 0 else {
                    self._limit -= 1
                    return self._factory()
                }
                
                let promise = Promise<Res>(context: context)
                self._queue.append(promise)
                
                return promise.future
            }
        }
    }
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
    
    public func rows() -> Future<[Row]> {
        return accumulate(rows: [Row]())
    }
    
    public func dictionaries() -> Future<[[String: Any?]]> {
        return columns.flatMap { cols in
            self.rows().map { rows in
                rows.map { row in
                    cols.zipWith(other: row).map(tuple).dictionary
                }
            }
        }
    }
}

public class ConnectionPool : Connection {
    private let _rp: ResourcePool<Connection>
    private let _pool: ResourcePool<Connection>.Iterator
    
    public init(size: UInt, connectionFactory:@escaping ()->Future<Connection>) {
        _rp = ResourcePool(context: ExecutionContext(kind: .serial), limit: size, factory: connectionFactory)
        _pool = _rp.makeIterator()
    }
    
    public func execute(query: String, parameters: [Any?], named: [String : Any?]) -> Future<ResultSet?> {
        return connection().flatMap { (connection, release) in
            connection.execute(query: query, parameters: parameters, named: named).onComplete { _ in
                release()
            }
        }
    }
    
    //returns (connection, release)
    public func connection() -> Future<(Connection, ()->())> {
        let rp = _rp
        return _pool.next()!.map { connection in
            (connection, {rp.reclaim(resource: connection)})
        }
    }
}

public class RDBC : ConnectionFactory, PoolFactory {
    public static let POOL_SIZE = "_k_poolSize"
    
    private var _drivers = [String:Driver]()
    private let _contextFactory:()->ExecutionContextProtocol
    
    public init() {
        _contextFactory = {ExecutionContext(kind: .serial)}
    }
    
    public func register(driver: Driver) {
        _drivers[driver.proto] = driver
    }
    
    public func register(driver: SyncDriver) {
        register(driver: AsyncDriver(driver: driver, contextFactory: _contextFactory))
    }
    
    public func pool(url:String, params:Dictionary<String, String>) throws -> ConnectionPool {
        let driver = try self.driver(url: url, params: params)
        
        let poolSize = params[RDBC.POOL_SIZE].flatMap {UInt($0)}.flatMap { size in
            [driver.poolSizeLimit, size].max()
        } ?? driver.poolSizeRecommended
        
        return ConnectionPool(size: poolSize) {
            driver.connect(url: url, params: params)
        }
    }
    
    public func driver(url _url: String, params: Dictionary<String, String>) throws -> Driver {
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
        return future(context: _contextFactory()) {
            try self.driver(url: url, params: params)
        }.flatMap { driver in
            driver.connect(url: url, params: params)
        }
    }
}
