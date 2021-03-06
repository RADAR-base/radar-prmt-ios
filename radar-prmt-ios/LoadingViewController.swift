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

class LoadingViewController: UIViewController {
    private let disposeBag = DisposeBag()

    override func viewWillAppear(_ animated: Bool) {
        self.navigationController?.isToolbarHidden = true
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        appDelegate.latestConfig
            .filter { !$0.config.isEmpty && $0.isAuthLoaded }
            .take(1)
            .observeOn(MainScheduler.instance)
            .subscribe(onNext: { [weak self] authConfig in
                guard let user = authConfig.user, authConfig.auth != nil else {
                    os_log("Loaded without authentication")
                    self?.performSegue(withIdentifier: "login", sender: self)
                    return
                }
                if !user.privacyPolicyAccepted {
                    os_log("Still need to accept privacy policy")
                    self?.performSegue(withIdentifier: "loadPrivacyPolicy", sender: self)
                } else {
                    os_log("Loaded with valid authentication")
                    self?.performSegue(withIdentifier: "mainFromLoading", sender: self)
                }
            })
            .disposed(by: disposeBag)

        appDelegate.config.fetch(withDelay: 14400)
        // Do any additional setup after loading the view, typically from a nib.
    }
}
