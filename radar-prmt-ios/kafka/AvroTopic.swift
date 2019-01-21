//
//  File.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 03/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import CoreData

struct AvroTopic {
    let name: String
    let valueSchema: Schema
    var priority: Int16 = 0
    let valueSchemaString: String
    
    init(name: String, valueSchema: String) throws {
        self.name = name
        let parsedSchema = try Schema(json: valueSchema)
        self.valueSchema = parsedSchema
        guard let canonicalForm = parsedSchema.jsonString() else {
            throw AvroConversionError.mismatch
        }
        self.valueSchemaString = canonicalForm
    }
}

extension AvroTopic: Equatable {
    static func == (lhs: AvroTopic, rhs: AvroTopic) -> Bool {
        return lhs.name == rhs.name
            && lhs.valueSchema == rhs.valueSchema
    }
}

extension AvroTopic: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(name)
    }
}
