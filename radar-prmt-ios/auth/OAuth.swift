//
//  File.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 20/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import Security

protocol Authorizer {
    func addAuthorization(to: inout URLRequest)
    func invalidate()
    func ensureValid(otherwiseRun callback: @escaping () -> Void) -> Bool
    var projectId: String { get }
    var userId: String { get }
}

class OAuth : Authorizer {
    var token: String? = nil

    func addAuthorization(to request: inout URLRequest) {
        request.addValue("Bearer \(token ?? "")", forHTTPHeaderField: "Authorization")
    }

    func ensureValid(otherwiseRun callback: @escaping () -> Void) -> Bool {
        if token == nil {
            token = "aa"
            DispatchQueue.global(qos: .background).asyncAfter(deadline: .now() + 1.0, execute: callback)
            return false
        } else {
            return true
        }
    }

    var userId = "u"
    var projectId = "p"
    func invalidate() {
        token = nil
    }
}

public class Strongbox {
    let keyPrefix: String
    public var lastStatus = errSecSuccess
    
    public convenience init() {
        self.init(keyPrefix: Bundle.main.bundleIdentifier)
    }
    
    public init(keyPrefix: String?) {
        if let prefix = keyPrefix {
            self.keyPrefix = prefix + "."
        } else {
            self.keyPrefix = "."
        }
    }

    /**
     Insert an object into the keychain. Pass `nil` for object to remove it from the keychain.
     
     - returns:
     Boolean indicating success or failure
     
     - parameters:
     - object: data to store. Pass `nil` to remove previous value for key
     - key: key with which to associate the stored value, or key to remove if `object` is nil
     - accessibility: keychain accessibility of item once stored
     
     */
    public func archive(_ object: Any?, key: String, accessibility: CFString = kSecAttrAccessibleWhenUnlocked) -> Bool
    {
        guard let _=object as? NSSecureCoding else {
            // The optional is empty, so remove the key
            return remove(key: key, accessibility: accessibility)
        }
        
        let data = NSMutableData()
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        archiver.encode(object, forKey: key)
        archiver.finishEncoding()
        
        return self.set(data, key: key, accessibility: accessibility)
    }
    
    /**
     Convenience method for removing a previously stored key.
     
     - returns:
     Boolean indicating success or failure
     
     - parameters:
     - key: key for which to remove the stored value
     - accessibility: keychain accessibility of item
     */
    @discardableResult
    public func remove(key: String, accessibility: CFString = kSecAttrAccessibleWhenUnlocked) -> Bool {
        let query = self.createQuery(forKey: key)
        lastStatus = SecItemDelete(query as CFDictionary)
        
        return lastStatus == errSecSuccess || lastStatus == errSecItemNotFound
    }
    
    /**
     Retrieve an object from the keychain.
     
     - returns:
     Re-constituted object from keychain, or nil if the key was not found. Since the method returns `Any?` it is
     the caller's responsibility to cast the result to the type expected.
     
     - parameters:
     - key: the key to use to locate the stored value
     */
    public func unarchive(objectForKey key:String) -> Any? {
        guard let data = self.data(forKey: key) else {
            return nil
        }
        
        do {
            let unarchiver = try NSKeyedUnarchiver(forReadingFrom: data as Data)
            return unarchiver.decodeObject(forKey: key)
        } catch {
            remove(key: key)
            return nil
        }
    }
    
    // MARK: Private functions to do all the work
    
    func set(_ data: NSMutableData, key: String, accessibility: CFString = kSecAttrAccessibleWhenUnlocked) -> Bool {
        let dict = [kSecClass as String: kSecClassGenericPassword,
                    kSecAttrService as String: keyPrefix + key,
                    kSecAttrAccessible as String: accessibility,
                    kSecValueData as String: data] as CFDictionary

        lastStatus = SecItemAdd(dict, nil)
        if lastStatus == errSecDuplicateItem {
            lastStatus = SecItemDelete(createQuery(forKey: key))
            if lastStatus == errSecSuccess {
                lastStatus = SecItemAdd(dict, nil)
            }
        }
        
        return lastStatus == errSecSuccess
    }
    
    func createQuery(forKey key: String) -> CFDictionary {
        return [kSecAttrService as String: keyPrefix + key,
                kSecClass as String: kSecClassGenericPassword,
                kSecReturnData as String: true] as CFDictionary
    }
    
    func data(forKey key:String) -> Data? {
        var data: AnyObject?
        lastStatus = SecItemCopyMatching(createQuery(forKey: key), &data)
        
        return data as? Data
    }
    
}
