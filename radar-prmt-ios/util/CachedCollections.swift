//
//  CachedDictionary.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 24/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

struct DictionaryCache<Key, Value> where Key: Hashable {
    fileprivate var backing: [Key: CacheItem<Value>]
    let defaultInvalidationInterval: TimeInterval

    init(minimumCapacity: Int = 10, invalidateAfter interval: TimeInterval = 3600.0) {
        backing = [Key: CacheItem<Value>](minimumCapacity: minimumCapacity)
        self.defaultInvalidationInterval = interval
    }

    public subscript(key: Key) -> Value? {
        get {
            return backing[key]?.validValue
        }
        set(newValue) {
            if let newValue = newValue {
                backing[key] = CacheItem(newValue, invalidateAfter: defaultInvalidationInterval)
            } else {
                backing[key] = nil
            }
        }
    }

    public subscript(key: Key, default defaultValue: @autoclosure () -> Value) -> Value {
        get {
            return self[key] ?? defaultValue()
        }
        set(newValue) {
            self[key] = newValue
        }
    }

    @discardableResult
    mutating func updateValue(_ value: Value, forKey key: Key, invalidateAfter interval: TimeInterval? = nil) -> Value? {
        let item = CacheItem(value, invalidateAfter: interval ?? self.defaultInvalidationInterval)
        return backing.updateValue(item, forKey: key)?.validValue
    }

    @discardableResult
    mutating func removeValue(forKey key: Key) -> Value? {
        return backing.removeValue(forKey: key)?.validValue
    }

    func invalidationInterval(forKey key: Key) -> TimeInterval? {
        return backing[key]?.invalidationInterval
    }

    mutating func clean() {
        backing = backing.filter { entry in entry.value.isValid() }
    }
}

extension DictionaryCache: Sequence {
    func makeIterator() -> Iterator {
        return Iterator(subIterator: backing.makeIterator())
    }

    struct Iterator: IteratorProtocol {
        fileprivate var subIterator: Dictionary<Key, CacheItem<Value>>.Iterator

        mutating func next() -> (key: Key, value: Value)? {
            while let entry = subIterator.next() {
                if let value = entry.value.validValue {
                    return (key: entry.key, value: value)
                }
            }
            return nil
        }
    }
}

struct SetCache<Element> where Element: Hashable {
    fileprivate var backing: DictionaryCache<Element, Element>

    init(minimumCapacity: Int = 10, invalidateAfter interval: TimeInterval = 3600.0) {
        backing = DictionaryCache(minimumCapacity: minimumCapacity, invalidateAfter: interval)
    }

    @discardableResult
    mutating func insert(_ element: Element) -> (inserted: Bool, memberAfterInsert: Element) {
        if let oldElement = backing[element] {
            return (inserted: false, memberAfterInsert: oldElement)
        } else {
            backing[element] = element
            return (inserted: true, memberAfterInsert: element)
        }
    }

    @discardableResult
    mutating func update(with element: Element, invalidateAfter interval: TimeInterval? = nil) -> Element? {
        return backing.updateValue(element, forKey: element, invalidateAfter: interval)
    }

    func contains(_ member: Element) -> Bool {
        return backing.contains { $0.value == member }
    }

    @discardableResult
    mutating func remove(_ member: Element) -> Element? {
        return backing.removeValue(forKey: member)
    }

    func invalidationInterval(for element: Element) -> TimeInterval? {
        return backing.invalidationInterval(forKey: element)
    }

    mutating func clean() {
        backing.clean()
    }
}

extension SetCache: Sequence {
    func makeIterator() -> Iterator {
        return Iterator(subIterator: backing.makeIterator())
    }

    struct Iterator: IteratorProtocol {
        fileprivate var subIterator: DictionaryCache<Element, Element>.Iterator

        mutating func next() -> Element? {
            return subIterator.next()?.value
        }
    }
}

private struct CacheItem<Item> {
    var validValue: Item? {
        return isValid() ? value : nil
    }
    private let value: Item

    private let validUntil: Date
    let invalidationInterval: TimeInterval

    init(_ value: Item, invalidateAfter interval: TimeInterval = 3600.0) {
        self.value = value
        self.invalidationInterval = interval
        self.validUntil = Date().addingTimeInterval(interval)
    }

    func isValid() -> Bool {
        return Date() <= validUntil
    }
}

extension CacheItem: Hashable, Equatable where Item: Hashable {
    func hash(hasher: inout Hasher) {
        hasher.combine(validValue)
    }

    static func ==(_ lhs: CacheItem<Item>, _ rhs: CacheItem<Item>) -> Bool {
        return lhs.validValue == rhs.validValue
    }
}
