//
//  Hello_ConvexApp.swift
//  Hello Convex
//
//  Created by Lee Danilek on 7/10/22.
//

import SwiftUI

@main
struct Hello_ConvexApp: App {
    @StateObject var counter = useQuery("getCounter");
    var incrementCounter = useMutation("incrementCounter");
    
    var body: some Scene {
        WindowGroup {
            ContentView(counter: counter, incrementCounter: incrementCounter)
        }
    }
}
