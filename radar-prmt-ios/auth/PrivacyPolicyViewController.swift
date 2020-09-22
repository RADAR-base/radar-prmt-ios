//
//  PrivacyPolicyViewController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 12/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import UIKit
import SafariServices
import RxSwift

class PrivacyPolicyViewController : UIViewController, SFSafariViewControllerDelegate {
    @IBOutlet weak var baseUrlLabel: UILabel!
    @IBOutlet weak var projectIdLabel: UILabel!
    @IBOutlet weak var userIdLabel: UILabel!
    let disposeBag = DisposeBag()
    var privacyPolicyUrl: URL? = nil

    override func viewDidLoad() {
        self.navigationItem.hidesBackButton = true
        appDelegate.authController.user
            .compactMap { $0 }
            .observeOn(MainScheduler.instance)
            .subscribe(onNext: { [weak self] user in
                guard let self = self else { return }
                self.baseUrlLabel?.text = user.baseUrl.absoluteString
                self.projectIdLabel?.text = user.projectId
                self.userIdLabel?.text = user.userId
                self.privacyPolicyUrl = user.privacyPolicyUrl
            })
            .disposed(by: disposeBag)
    }

    @IBAction func acceptPrivacyPolicy(_ sender: Any) {
        if let userId = self.userIdLabel?.text {
            appDelegate.authController.acceptPrivacyPolicy(for: userId)
                .subscribeOn(MainScheduler.instance)
                .subscribe(onNext: { [weak self] user in
                    if user.privacyPolicyAccepted {
                        self?.performSegue(withIdentifier: "acceptedPrivacyPolicy", sender: self)
                    }
                })
                .disposed(by: disposeBag)
        }
    }

    @IBAction func showDataSources(_ sender: Any) {
        guard let privacyPolicyUrl = privacyPolicyUrl else { return }
        let sfView = SFSafariViewController(url: privacyPolicyUrl)
        sfView.delegate = self
        present(sfView, animated: true)
    }

    @IBAction func showPrivacyPolicy(_ sender: Any) {
        guard let privacyPolicyUrl = privacyPolicyUrl else { return }
        let sfView = SFSafariViewController(url: privacyPolicyUrl)
        sfView.delegate = self
        present(sfView, animated: true)
    }
}
