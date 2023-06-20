//
//  URLRequest+RESTJSON.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

extension URLRequest {
    mutating func addBasicAuthentication(identifiedBy username: String, authenticatedWith password: String) {
        let credentials = "\(username):\(password)"
            .data(using: .utf8)!
            .base64EncodedString()

        addValue("Basic \(credentials)", forHTTPHeaderField: "Authorization")
    }

    /// Populate the HTTPBody of `application/x-www-form-urlencoded` request
    ///
    /// - parameter parameters:   A dictionary of keys and values to be added to the request

    mutating func postForm(_ form: [String: String]) {
        httpMethod = "POST"
        addValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        httpBody = form.map { (key, value) -> String in
            let encodedKey = key.addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed)!
            let encodedValue = value.addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed)!
            return "\(encodedKey)=\(encodedValue)"
            }
            .joined(separator: "&")
            .data(using: .utf8)
    }

    mutating func postJson<T: Encodable>(_ codable: T) throws {
        let encoder = JSONEncoder()
        httpMethod = "POST"
        httpBody = try encoder.encode(codable)
        addValue("application/json; charset=utf-8", forHTTPHeaderField: "Content-Type")
        addValue("application/json", forHTTPHeaderField: "Accept")
    }
}

extension CharacterSet {
    /// Character set containing characters allowed in query value as outlined in RFC 3986.
    ///
    /// RFC 3986 states that the following characters are "reserved" characters.
    ///
    /// - General Delimiters: ":", "#", "[", "]", "@", "?", "/"
    /// - Sub-Delimiters: "!", "$", "&", "'", "(", ")", "*", "+", ",", ";", "="
    ///
    /// In RFC 3986 - Section 3.4, it states that the "?" and "/" characters should not be escaped to allow
    /// query strings to include a URL. Therefore, all "reserved" characters with the exception of "?" and "/"
    /// should be percent-escaped in the query string.
    ///
    /// - parameter string: The string to be percent-escaped.
    ///
    /// - returns: The percent-escaped string.

    static let urlQueryValueAllowed: CharacterSet = {
        let generalDelimitersToEncode = ":#[]@" // does not include "?" or "/" due to RFC 3986 - Section 3.4
        let subDelimitersToEncode = "!$&'()*+,;="

        var allowed = CharacterSet.urlQueryAllowed
        allowed.remove(charactersIn: generalDelimitersToEncode + subDelimitersToEncode)

        return allowed
    }()
}
