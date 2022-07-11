//
//  ContentView.swift
//  Hello Convex
//
//  Created by Lee Danilek on 7/10/22.
//

import SwiftUI

struct ContentView: View {
    @StateObject var counter: ObservableInt
    var incrementCounter: ([Value]) -> Void = { _ in }
    
    var body: some View {
        VStack{
            Text("Counter: \(counter.value ?? 0)")
                .padding()
            Button("Increment Counter") {
                incrementCounter([1])
            }
        }
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView(counter: ObservableInt())
    }
}
