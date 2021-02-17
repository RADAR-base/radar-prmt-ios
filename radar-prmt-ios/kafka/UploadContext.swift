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
    func start(element: UploadQueueElement, upload: RecordSetUpload, schemas: (SchemaMetadata, SchemaMetadata)) throws -> UploadHandle
}

protocol UploadHandle {
    var topic: String { get }
    var priority: Int16 { get }
    var headers: [String: String] { get }
    var count: Int { get }
    var uploadQueueElement: UploadQueueElement { get }
    var isComplete: Bool { get }
    var mediumHandle: MediumHandle { get }
    func add(value: AvroValue) throws
    func finalize() throws
}

class JsonUploadContext: UploadContext {
    let user: User
    let medium: RequestMedium
    let encoder: AvroEncoder = GenericAvroEncoder(encoding: .json)

    init(user: User, medium: RequestMedium) {
        self.user = user
        self.medium = medium
    }

    func start(element: UploadQueueElement, upload: RecordSetUpload, schemas: (SchemaMetadata, SchemaMetadata)) throws -> UploadHandle {
        let handle = try medium.start(upload: upload)
        let key = ["projectId": user.projectId, "userId": user.userId, "sourceId": upload.dataGroup!.sourceId!].toAvro()
        return try JsonUploadHandle(upload: element, priority: upload.topic!.priority, mediumHandle: handle, schemas: schemas, encoder: encoder, key: key)
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

    let headers = ["Content-Type": "application/vnd.kafka.avro.v2+json"]

    var isComplete: Bool {
        get { return mediumHandle.isComplete }
    }

    let mediumHandle: MediumHandle
    let encoder: AvroEncoder

    let keySchema: SchemaMetadata
    let valueSchema: SchemaMetadata

    var topic: String {
        get {
            return uploadQueueElement.topic
        }
    }
    var first: Bool = true
    var count: Int = 0
    var keyData: Data?
    let priority: Int16
    let uploadQueueElement: UploadQueueElement

    init(upload: UploadQueueElement, priority: Int16, mediumHandle: MediumHandle, schemas: (SchemaMetadata, SchemaMetadata), encoder: AvroEncoder, key: AvroValue) throws {
        self.uploadQueueElement = upload
        self.priority = priority
        self.mediumHandle = mediumHandle
        self.encoder = encoder
        (keySchema, valueSchema) = schemas
        if isComplete {
            keyData = nil
        } else {
            keyData = try encoder.encode(key, as: keySchema.schema)
        }
        try mediumHandle.append(data: JsonUploadHandle.requestKeySchema)
        try mediumHandle.append(string: String(keySchema.id))
        try mediumHandle.append(data: JsonUploadHandle.requestValueSchema)
        try mediumHandle.append(string: String(valueSchema.id))
        try mediumHandle.append(data: JsonUploadHandle.requestRecord)
    }

    func add(value: AvroValue) throws {
        guard let encodedValue = try? encoder.encode(value, as: valueSchema.schema) else {
            os_log("Cannot convert %@ value %@ to schema %@. Skipping", topic, value.description, valueSchema.schema.description)
            return
        }
        guard let keyData = keyData else { return }
        if (first) {
            first = false
        } else {
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


class BinaryUploadHandle: UploadHandle {
    let headers = ["Content-Type": "application/vnd.radarbase.avro.v1+binary"]

    static let separator = ",".data(using: .ascii)!
    static let recordKey = "{\"key\":".data(using: .ascii)!
    static let recordValue = ",\"value\":".data(using: .ascii)!
    static let recordEnd = "}".data(using: .ascii)!
    static let requestKeySchema = "{\"key_schema_id\":".data(using: .ascii)!
    static let requestValueSchema = ",\"value_schema_id\":".data(using: .ascii)!
    static let requestRecord = ",\"records\":[".data(using: .ascii)!
    static let requestEnd = "]}".data(using: .ascii)!

    var isComplete: Bool {
        get { return mediumHandle.isComplete }
    }

    let mediumHandle: MediumHandle
    let encoder: AvroEncoder

    let keySchema: SchemaMetadata
    let valueSchema: SchemaMetadata

    var topic: String {
        get {
            return uploadQueueElement.topic
        }
    }
    var first: Bool = true
    var count: Int = 0
    var keyData: Data?
    let priority: Int16
    let uploadQueueElement: UploadQueueElement

    init(upload: UploadQueueElement, priority: Int16, mediumHandle: MediumHandle, schemas: (SchemaMetadata, SchemaMetadata), encoder: AvroEncoder, key: AvroValue) throws {
        self.uploadQueueElement = upload
        self.priority = priority
        self.mediumHandle = mediumHandle
        self.encoder = encoder
        (keySchema, valueSchema) = schemas
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
        print("**%uploadTask / request", request)
        request.httpMethod = "POST"
        handle.headers.forEach { (key, value) in
            request.setValue(value, forHTTPHeaderField: key)
        }

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
