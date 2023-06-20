//
//  AuthStorage.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 03/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import Valet

extension Valet {
    func store<T: Codable>(codable value: T, forKey key: String) throws {
        let data = try JSONEncoder().encode(value)
        try setObject(data, forKey: key)
    }
    func load<T: Codable>(type: T.Type, forKey key: String) throws -> T? {
        guard let data = try? object(forKey: key) else {
            return nil
        }
        return try JSONDecoder().decode(type, from: data)
    }
}
