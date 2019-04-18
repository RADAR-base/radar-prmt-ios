//
//  SchemaRegistryClient.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log

class SchemaRegistryClient {
    static let CACHE_TIMEOUT = 3600 as TimeInterval

    let schemaUrl: URL
    private var cache: DictionaryCache<String, FetchedSchemaPair>
    private let queue: DispatchQueue

    init(baseUrl: URL) {
        var url = baseUrl
        url.appendPathComponent("schema", isDirectory: true)
        url.appendPathComponent("subjects", isDirectory: true)
        self.schemaUrl = url
        self.cache = DictionaryCache(invalidateAfter: SchemaRegistryClient.CACHE_TIMEOUT)
        self.queue = DispatchQueue(label: "Schema Registry", qos: .background)
    }

    func requestSchemas(for topic: String, executing block: @escaping (FetchedSchemaPair) -> ()) {
        let queue = self.queue
        queue.async { [weak self] in
            guard let self = self else {
                var pair = FetchedSchemaPair()
                pair.isUpToDate = true
                block(pair)
                return
            }
            var pair = self.cache[topic, default: FetchedSchemaPair()]

            if pair.isComplete {
                block(pair)
                return
            }

            pair.reset()
            self.cache[topic] = pair

            if !pair.didUpdateKeySchema {
                self.makeRequest(for: topic, part: .key, executing: block)
            }

            if !pair.didUpdateValueSchema {
                self.makeRequest(for: topic, part: .value, executing: block)
            }
        }
    }

    private func makeRequest(for topic: String, part: RecordPart, executing block: @escaping (FetchedSchemaPair) -> ()) {
        var url = schemaUrl
        url.appendPathComponent("\(topic)-\(part.rawValue)", isDirectory: true)
        url.appendPathComponent("versions", isDirectory: true)
        url.appendPathComponent("latest", isDirectory: false)
        var request = URLRequest(url: url)
        request.addValue("application/vnd.schemaregistry.v1+json", forHTTPHeaderField: "Accept")
        os_log("Requesting schema %@-%@", topic, part.rawValue)
        URLSession.shared.dataTask(with: request, completionHandler: { [weak self] (data, response, error) in
            guard let response = response as? HTTPURLResponse, response.statusCode == 200, let data = data else {
                os_log("Failed to retrieve schema at %@", type: .error, url.absoluteString)
                self?.updatePair(part: part, for: topic, with: nil, reportCompleted: block)
                return
            }

            let schemaMetadata: SchemaMetadata?

            if let jsonObject = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                    let schemaId = jsonObject["id"] as? Int,
                    let schemaVersion = jsonObject["version"] as? Int,
                    let schemaString = jsonObject["schema"] as? String,
                    let schema = try? Schema(json: schemaString) {
                schemaMetadata = SchemaMetadata(id: schemaId, version: schemaVersion, schema: schema)
            } else {
                os_log("Failed to parse schema at %@", type: .error, url.absoluteString)
                schemaMetadata = nil
            }

            self?.updatePair(part: part, for: topic, with: schemaMetadata, reportCompleted: block)
        }).resume()
    }

    private func updatePair(part: RecordPart, for topic: String, with schemaMetadata: SchemaMetadata?, reportCompleted block: @escaping (FetchedSchemaPair) -> ()) {
        self.queue.async { [weak self] in
            guard let self = self else { return }
            var pair: FetchedSchemaPair = self.cache[topic, default: FetchedSchemaPair()]

            switch part {
            case .key:
                pair.keySchema = schemaMetadata
                pair.didUpdateKeySchema = true
            case .value:
                pair.valueSchema = schemaMetadata
                pair.didUpdateValueSchema = true
            }
            self.cache[topic] = pair

            os_log("Requested schema: {key: %@, value: %@}", pair.keySchema?.schema.description ?? "??", pair.valueSchema?.schema.description ?? "??")
            if pair.isUpToDate {
                block(pair)
            }
        }
    }

    private enum RecordPart: String {
        case key
        case value
    }
}

struct FetchedSchemaPair {
    var didUpdateKeySchema: Bool = false
    var didUpdateValueSchema: Bool = false
    var keySchema: SchemaMetadata? = nil
    var valueSchema: SchemaMetadata? = nil

    var isUpToDate: Bool {
        get {
            return didUpdateKeySchema && didUpdateValueSchema
        }
        mutating set(value) {
            didUpdateKeySchema = value
            didUpdateValueSchema = value
        }
    }

    mutating func reset() {
        didUpdateKeySchema = keySchema != nil
        didUpdateValueSchema = valueSchema != nil
    }

    var isComplete: Bool {
        return keySchema != nil && valueSchema != nil
    }
}

struct SchemaMetadata {
    let id: Int
    let version: Int
    let schema: Schema
}
