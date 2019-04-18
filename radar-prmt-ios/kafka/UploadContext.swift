//
//  UploadContext.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 18/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log

protocol UploadContext {
    func start(upload: RecordSetUpload, schemas: FetchedSchemaPair) throws -> UploadHandle
}

protocol UploadHandle {
    var topic: String { get }
    var contentType: String { get }
    var count: Int { get }
    var isComplete: Bool { get }
    var mediumHandle: MediumHandle { get }
    func add(value: AvroValue) throws
    func finalize() throws
}

class JsonUploadContext: UploadContext {
    let auth: Authorizer
    let medium: RequestMedium
    let encoder: AvroEncoder = GenericAvroEncoder(encoding: .json)

    init(auth: Authorizer, medium: RequestMedium) {
        self.auth = auth
        self.medium = medium
    }

    func start(upload: RecordSetUpload, schemas: FetchedSchemaPair) throws -> UploadHandle {
        let handle = try medium.start(upload: upload)
        let key = ["projectId": auth.projectId, "userId": auth.userId, "sourceId": upload.dataGroup!.sourceId!].toAvro()
        return try JsonUploadHandle(topic: upload.topic!.name!, mediumHandle: handle, schemas: schemas, encoder: encoder, key: key)
    }
}

class JsonUploadHandle: UploadHandle {
    static let separator = ",".data(using: .ascii)!
    static let recordKey = "{\"key\":".data(using: .ascii)!
    static let recordValue = ",\"value\":".data(using: .ascii)!
    static let recordEnd = "}".data(using: .ascii)!
    static let requestKeySchema = "{\"key_schema_id\":".data(using: .ascii)!
    static let requestValueSchema = ",\"value_schema_id\":".data(using: .ascii)!
    static let requestRecord = ",\"records\":[".data(using: .ascii)!
    static let requestEnd = "]}".data(using: .ascii)!

    let contentType: String = "application/vnd.kafka.avro.v2+json"

    var isComplete: Bool {
        get { return mediumHandle.isComplete }
    }


    let mediumHandle: MediumHandle
    let encoder: AvroEncoder

    let keySchema: SchemaMetadata
    let valueSchema: SchemaMetadata

    var topic: String
    var first: Bool = true
    var count: Int = 0
    var keyData: Data?

    init(topic: String, mediumHandle: MediumHandle, schemas: FetchedSchemaPair, encoder: AvroEncoder, key: AvroValue) throws {
        self.mediumHandle = mediumHandle
        self.encoder = encoder
        self.topic = topic
        keySchema = schemas.keySchema!
        valueSchema = schemas.valueSchema!
        if isComplete {
            keyData = nil
        } else {
            keyData = try encoder.encode(key, as: keySchema.schema)
        }
    }

    func add(value: AvroValue) throws {
        guard let encodedValue = try? encoder.encode(value, as: valueSchema.schema) else {
            os_log("Cannot convert %@ value %@ to schema %@. Skipping", topic, value.description, valueSchema.schema.description)
            return
        }
        guard let keyData = keyData else { return }
        if (first) {
            try mediumHandle.append(data: JsonUploadHandle.separator)
        }
        try mediumHandle.append(data: JsonUploadHandle.recordKey)
        try mediumHandle.append(data: keyData)
        try mediumHandle.append(data: JsonUploadHandle.recordValue)
        try mediumHandle.append(data: encodedValue)
        try mediumHandle.append(data: JsonUploadHandle.recordEnd)
        count += 1
    }

    func finalize() throws {
        guard !isComplete else { return }
        try mediumHandle.append(data: JsonUploadHandle.requestEnd)
        try mediumHandle.finalize()
    }
}

extension URLSession {
    func uploadTask(with request: URLRequest, from handle: UploadHandle) -> URLSessionUploadTask {
        var request = request
        request.httpMethod = "POST"
        request.setValue(handle.contentType, forHTTPHeaderField: "Content-Type")

        switch handle.mediumHandle {
        case let fileHandle as FileMediumHandle:
            return uploadTask(with: request, fromFile: fileHandle.file)
        case let dataHandle as DataMediumHandle:
            return uploadTask(with: request, from: dataHandle.data)
        default:
            fatalError("Cannot send data type")
        }
    }
}
