//
//  ViewController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import UIKit
import RxSwift
import os.log

class MainViewController: UIViewController {
    private let disposeBag = DisposeBag()
    @IBOutlet weak var projectIdLabel: UILabel!
    @IBOutlet weak var userIdLabel: UILabel!
    @IBOutlet weak var serverStatusLabel: UILabel!

    override func viewWillAppear(_ animated: Bool) {
        navigationItem.hidesBackButton = true;
    }

    override func viewDidLoad() {
        super.viewDidLoad()

        appDelegate.authController.user
            .compactMap { $0 }
            .subscribeOn(MainScheduler.instance)
            .subscribe(onNext: { [weak self] user in
                guard let self = self else { return }
                self.projectIdLabel.text = user.projectId
                self.userIdLabel.text = user.userId
            })
            .disposed(by: disposeBag)

        let formatter = DateFormatter()
        formatter.dateStyle = .none
        formatter.timeStyle = .medium

        appDelegate.lastServerStatus
            .throttle(.seconds(1), latest: true, scheduler: ConcurrentDispatchQueueScheduler(qos: .background))
            .observeOn(MainScheduler.instance)
            .subscribe(onNext: { [weak self] event in
                guard let self = self else { return }
                switch (event) {
                case .none:
                    self.serverStatusLabel.text = "–"
                case let .disconnected(date):
                    self.serverStatusLabel.text = "Disconnected at \(formatter.string(from: date))"
                case let .connected(date):
                    self.serverStatusLabel.text = "Connected at \(formatter.string(from: date))"
                case let .success(date):
                    self.serverStatusLabel.text = "Successful request at \(formatter.string(from: date))"
                case let .serverFailure(date, message):
                    let forcedMessage = message ?? "Reason unknown"
                    self.serverStatusLabel.text = "Server failure \(formatter.string(from: date)): \(forcedMessage)"
                case let .appFailure(date, message):
                    self.serverStatusLabel.text = "Message failure \(formatter.string(from: date)): \(message)"
                }
            })
            .disposed(by: disposeBag)
    }

    @IBAction func logOut(_ sender: Any) {
        appDelegate.authController.reset()
        self.performSegue(withIdentifier: "resetLogin", sender: self)
    }
}
