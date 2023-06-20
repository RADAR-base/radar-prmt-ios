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
import RxSwift
import RxCocoa
import RxSwiftExt

class SchemaRegistryClient {
    static let CacheTimeout = 3600 as TimeInterval

    let schemaUrl: URL
    private var cache: DictionaryCache<String, SchemaMetadata>
    private let queue: DispatchQueue
    private let decoder = JSONDecoder()

    init(baseUrl: URL) {
        var url = baseUrl
        url.appendPathComponent("schema", isDirectory: true)
        url.appendPathComponent("subjects", isDirectory: true)
        self.schemaUrl = url
        self.cache = DictionaryCache(invalidateAfter: SchemaRegistryClient.CacheTimeout)
        self.queue = DispatchQueue(label: "Schema Registry", qos: .background)
    }

    func requestSchemas(for topic: String) -> Observable<Result<(SchemaMetadata, SchemaMetadata), Error>> {
        return Observable
            .zip(self.makeRequest(for: topic, part: .key), self.makeRequest(for: topic, part: .value))
            .map { .success($0) }
            .catchError { Observable.just(.failure($0)) }
    }

    private func makeRequest(for topic: String, part: RecordPart) -> Observable<SchemaMetadata> {
        let decoder = self.decoder
        return Observable.deferred {
            let subject = "\(topic)-\(part.rawValue)"
            if let cached = self.cache[subject] {
                return .just(cached)
            }

            var url = self.schemaUrl
            url.appendPathComponent(subject, isDirectory: true)
            url.appendPathComponent("versions", isDirectory: true)
            url.appendPathComponent("latest", isDirectory: false)
            var request = URLRequest(url: url)
            request.addValue("application/vnd.schemaregistry.v1+json", forHTTPHeaderField: "Accept")
            os_log("Requesting schema %@-%@", topic, part.rawValue)
            return URLSession.shared.rx.data(request: request)
                .retry(.exponentialDelayed(maxCount: 5, initial: 1.0, multiplier: 10.0), shouldRetry: { error in
                    if case let RxCocoaURLError.httpRequestFailed(response: response, data: data) = error {
                        let bodyString: String
                        if let data = data {
                            bodyString = String(data: data.subdata(in: 0 ..< Swift.min(data.count, 255)), encoding: .utf8) ?? "??"
                        } else {
                            bodyString = "<empty>"
                        }
                        if response.statusCode >= 500 {
                            os_log("Retriable server error %d: %@", response.statusCode, bodyString)
                            return true
                        } else {
                            os_log("Unrecoverable server error %d: %@", response.statusCode, bodyString)
                            return false
                        }
                    } else {
                        return true
                    }
                })
                .map { data in
                    do {
                        return try decoder.decode(SchemaMetadata.self, from: data)
                    } catch {
                        os_log("Failed to parse schema at %@", type: .error, url.absoluteString)
                        throw RxCocoaURLError.deserializationError(error: error)
                    }
                }
                .do(onNext: { [weak self] schema in self?.cache[subject] = schema })
        }
    }

    private enum RecordPart: String {
        case key
        case value
    }
}

extension Schema: Decodable {
    public init(from decoder: Decoder) throws {
        try self.init(json: try decoder.singleValueContainer().decode(String.self))
    }
}

struct SchemaMetadata: Decodable {
    let id: Int
    let version: Int
    let schema: Schema
}
