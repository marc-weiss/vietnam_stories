//
//  ContentView.swift
//  WeblabTransformer
//
//  Created by Rob Reuss on 6/20/25.
//

import SwiftUI
import UniformTypeIdentifiers

struct Thread: Identifiable {
    let indexID: Int
    let threadTitle: String
    var posts: [Post] = []
    
    var id: Int { indexID }
}

struct Post: Identifiable {
    let postID: Int
    let createdAt: Date
    let postTitle: String
    let postBody: String
    let emailString: String
    let nameString: String
    let directory: String
    
    var id: Int { postID }
}

struct ContentView: View {
    @State private var threads: [Thread] = []
    @State private var files: [String] = []
    
    @State private var rootURL: URL?
    @State private var showingImporter = false

    private let subdirectoryNames = ["discuss", "discuss2"]
    
    var body: some View {
        List(threads) { thread in
            Section("\(thread.indexID): \(thread.threadTitle)") {
                ForEach(thread.posts) { post in
                    VStack(alignment: .leading) {
                        Text(post.postTitle).bold()
                        Text(post.postBody)
                        Text("— \(post.nameString), \(post.emailString)")
                            .italic()
                            .font(.caption)
                    }
                }
            }
        }
        .onAppear(perform: loadFiles)
        .fileImporter(isPresented: $showingImporter, allowedContentTypes: [.folder], allowsMultipleSelection: false) { result in
            switch result {
            case .success(let urls):
                if let url = urls.first, isDirectory(url) {
                    rootURL = url
                    loadFiles()
                } else {
                    print("Selected URL is not a directory.")
                }
            case .failure(let error):
                print("File import error: \(error)")
            }
        }
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Button("Choose Folder") {
                    showingImporter = true
                }
            }
        }
        .padding()
    }
    
    func loadFiles() {
        guard let rootURL = rootURL, isDirectory(rootURL) else { return }
        do {
            var loadedThreads: [Thread] = []
            for directoryURL in directoryURLs(from: rootURL) {
                let allFiles = try FileManager.default.contentsOfDirectory(atPath: directoryURL.path)
                if allFiles.contains("thread.index") {
                    let indexFileURL = directoryURL.appendingPathComponent("thread.index")
                    let content = try String(contentsOf: indexFileURL, encoding: .isoLatin1)
                    let directoryName = directoryName(for: directoryURL, rootURL: rootURL)

                    var directoryThreads = content.split(whereSeparator: \.isNewline)
                        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                        .filter { !$0.isEmpty }
                        .compactMap { line -> Thread? in
                            let parts = line.components(separatedBy: "\t")
                            guard parts.count == 2,
                                  let indexID = Int(parts[0].trimmingCharacters(in: .whitespaces)),
                                  !parts[1].isEmpty else { return nil }

                            return Thread(indexID: indexID, threadTitle: parts[1])
                        }
                        .sorted { $0.indexID < $1.indexID }

                    for index in directoryThreads.indices {
                        directoryThreads[index].posts = try loadPosts(
                            for: directoryThreads[index].indexID,
                            in: directoryURL,
                            directoryName: directoryName
                        )
                    }

                    loadedThreads.append(contentsOf: directoryThreads)
                } else if directoryURL == rootURL {
                    files = allFiles.filter { Int($0) != nil }
                        .sorted { Int($0)! < Int($1)! }
                }
            }

            threads = loadedThreads
            for thread in threads {
                print("THREAD: \(thread.indexID): \(thread.threadTitle)")
                for post in thread.posts {
                    print("  POST: \(post.postTitle), \(post.createdAt) \(post.nameString), \(post.emailString)")
                    print("  \(post.postBody)")
                    print("")
                }
                //print("\(thread.posts)")
                print("")
            }
            //exportToHTMLSite()
            exportToCSV()
        } catch {
            print("Error loading files: \(error)")
        }
    }
    
    func exportToHTMLSite() {
        let fileManager = FileManager.default
        let documentsURL = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first!
        let siteURL = documentsURL.appendingPathComponent("HTMLSite")
        
        try? fileManager.createDirectory(at: siteURL, withIntermediateDirectories: true)
        
        // Sort threads: first thread always at top, rest by post count descending
        let sortedThreads: [Thread]
        if let firstThread = threads.first {
            let rest = threads.dropFirst().sorted { $0.posts.count > $1.posts.count }
            sortedThreads = [firstThread] + rest
        } else {
            sortedThreads = []
        }
        
        // Generate Index HTML
        var indexHTML = """
        <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Threads Index</title><style>
        body { font-family:sans-serif; max-width:800px; margin:auto; padding:20px; line-height:1.4; }
        a { color:#0077cc; text-decoration:none; }
        .thread { margin-bottom:10px; }
        .byline { font-size:0.9em; color:#555; }
        </style></head><body>
        <h1>Discussion Threads</h1>
        """

        for thread in sortedThreads {
            guard let firstPostDate = thread.posts.min(by: { $0.createdAt < $1.createdAt })?.createdAt else { continue }
            let threadFile = "thread_\(thread.indexID).html"
            indexHTML += """
            <div class="thread">
                <a href="\(threadFile)"><strong>\(thread.threadTitle)</strong> (\(thread.posts.count))</a><br>
                <span class="byline">Started on \(formattedDate(firstPostDate))</span>
            </div>
            """
        }

        indexHTML += "</body></html>"

        do {
            try indexHTML.write(to: siteURL.appendingPathComponent("index.html"), atomically: true, encoding: .utf8)
        } catch {
            print("Failed to save index.html: \(error)")
        }

        // Generate individual thread pages
        for thread in threads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            var threadHTML = """
            <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>\(thread.threadTitle)</title><style>
            body { font-family:sans-serif; max-width:800px; margin:auto; padding:20px; line-height:1.4; }
            h2 { border-bottom:1px solid #ddd; }
            .post { border-bottom:1px solid #ccc; padding:10px 0; position:relative; }
            .byline { color:#555; font-size:0.9em; }
            pre { white-space:pre-wrap; font-family:inherit; }
            .permalink { position:absolute; top:10px; right:0; text-decoration:none; }
            </style></head><body>
            <a href="index.html">&larr; Back to Index</a>
            <h2>\(thread.threadTitle)</h2>
            """

            for post in sortedPosts {
                let anchor = "post\(post.postID)"
                threadHTML += """
                <div class="post" id="\(anchor)">
                    <a class="permalink" href="#\(anchor)">🔗</a>
                    <strong>\(post.postTitle)</strong><br>
                    <span class="byline">By \(post.nameString) on \(formattedDate(post.createdAt))</span>
                    <pre>\(normalizeLineBreaks(escapeHTML(post.postBody)))</pre>
                </div>
                """
            }

            threadHTML += "</body></html>"

            do {
                try threadHTML.write(to: siteURL.appendingPathComponent("thread_\(thread.indexID).html"), atomically: true, encoding: .utf8)
            } catch {
                print("Failed to save thread \(thread.indexID).html: \(error)")
            }
        }

        print("HTML site successfully created at \(siteURL.path)")
    }

    func exportToCSV() {
        let fileManager = FileManager.default
        let documentsURL = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first!
        let siteURL = documentsURL.appendingPathComponent("HTMLSite")

        try? fileManager.createDirectory(at: siteURL, withIntermediateDirectories: true)

        let sortedThreads: [Thread]
        if let firstThread = threads.first {
            let rest = threads.dropFirst().sorted { $0.posts.count > $1.posts.count }
            sortedThreads = [firstThread] + rest
        } else {
            sortedThreads = []
        }

        var csvRows: [String] = []
        csvRows.append([
            "post_created_at",
            "thread_title",
            "post_title",
            "post_email",
            "post_name",
            "post_body",
            "post_id",
            "thread_index_id"
        ].map(csvEscape).joined(separator: ","))

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withTimeZone]

        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                let normalizedBody = removeBlankLines(post.postBody)
                let row = [
                    formatter.string(from: post.createdAt),
                    thread.threadTitle,
                    post.postTitle,
                    post.emailString,
                    post.nameString,
                    normalizedBody,
                    String(post.postID),
                    String(thread.indexID)
                ].map(csvEscape).joined(separator: ",")
                csvRows.append(row)
            }
        }

        let csvContent = csvRows.joined(separator: "\n")
        let exportURL = siteURL.appendingPathComponent("threads.csv")
        do {
            try csvContent.write(to: exportURL, atomically: true, encoding: .utf8)
            print("CSV file successfully saved at \(exportURL.path)")
        } catch {
            print("Failed to save CSV file: \(error)")
        }

        let namesURL = siteURL.appendingPathComponent("post_names.csv")
        let nameRows = postsCSVNameRows(sortedThreads: sortedThreads)
        do {
            try nameRows.joined(separator: "\n").write(to: namesURL, atomically: true, encoding: .utf8)
            print("Post names CSV file successfully saved at \(namesURL.path)")
        } catch {
            print("Failed to save post names CSV file: \(error)")
        }

        let dedupedNamesURL = siteURL.appendingPathComponent("post_names_dedupped.csv")
        let dedupedNameRows = postsCSVDedupedNameRows(sortedThreads: sortedThreads)
        do {
            try dedupedNameRows.joined(separator: "\n").write(to: dedupedNamesURL, atomically: true, encoding: .utf8)
            print("Deduped post names CSV file successfully saved at \(dedupedNamesURL.path)")
        } catch {
            print("Failed to save deduped post names CSV file: \(error)")
        }

        let emailsURL = siteURL.appendingPathComponent("email_addresses_all.csv")
        let emailRows = postsCSVDedupedEmailRows(sortedThreads: sortedThreads)
        do {
            try emailRows.joined(separator: "\n").write(to: emailsURL, atomically: true, encoding: .utf8)
            print("Deduped email CSV file successfully saved at \(emailsURL.path)")
        } catch {
            print("Failed to save deduped email CSV file: \(error)")
        }

        let malformedEmailsURL = siteURL.appendingPathComponent("email_addresses_malformed.csv")
        let malformedEmailRows = postsCSVMalformedEmailRows(sortedThreads: sortedThreads)
        do {
            try malformedEmailRows.joined(separator: "\n").write(to: malformedEmailsURL, atomically: true, encoding: .utf8)
            print("Malformed email CSV file successfully saved at \(malformedEmailsURL.path)")
        } catch {
            print("Failed to save malformed email CSV file: \(error)")
        }

        let correctEmailsURL = siteURL.appendingPathComponent("email_addresses_correct.csv")
        let correctEmailRows = postsCSVCorrectEmailRows(sortedThreads: sortedThreads)
        do {
            try correctEmailRows.joined(separator: "\n").write(to: correctEmailsURL, atomically: true, encoding: .utf8)
            print("Correct email CSV file successfully saved at \(correctEmailsURL.path)")
        } catch {
            print("Failed to save correct email CSV file: \(error)")
        }

        exportAnalysis(sortedThreads: sortedThreads, siteURL: siteURL)
    }

    func formattedDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter.string(from: date)
    }

    func escapeHTML(_ text: String) -> String {
        text.replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\"", with: "&quot;")
            .replacingOccurrences(of: "'", with: "&#39;")
    }

    func normalizeLineBreaks(_ text: String) -> String {
        let paragraphs = text.components(separatedBy: .newlines)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
        return paragraphs.joined(separator: "\n\n")
    }

    func csvEscape(_ value: String) -> String {
        let normalized = value.replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
        let escaped = normalized.replacingOccurrences(of: "\"", with: "\"\"")
        return "\"\(escaped)\""
    }

    func removeBlankLines(_ text: String) -> String {
        let normalized = text.replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
        let lines = normalized.components(separatedBy: "\n")
        var result: [String] = []

        for line in lines {
            let isEmpty = line.trimmingCharacters(in: .whitespaces).isEmpty
            if !isEmpty {
                result.append(line)
            }
        }

        return result.joined(separator: "\n")
    }

    func isDirectory(_ url: URL) -> Bool {
        (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) ?? false
    }

    func directoryURLs(from rootURL: URL) -> [URL] {
        var urls: [URL] = [rootURL]
        for name in subdirectoryNames {
            let subdirectoryURL = rootURL.appendingPathComponent(name)
            if isDirectory(subdirectoryURL) {
                urls.append(subdirectoryURL)
            }
        }
        return urls
    }

    func directoryName(for directoryURL: URL, rootURL: URL) -> String {
        if directoryURL == rootURL {
            return ""
        }
        return directoryURL.lastPathComponent
    }

    func postsCSVNameRows(sortedThreads: [Thread]) -> [String] {
        var rows: [String] = []
        rows.append(csvEscape("post_name"))
        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                rows.append(csvEscape(post.nameString))
            }
        }
        return rows
    }

    func postsCSVDedupedNameRows(sortedThreads: [Thread]) -> [String] {
        var seen: Set<String> = []
        var rows: [String] = []
        rows.append(csvEscape("post_name"))

        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                let canonical = post.nameString.lowercased()
                if !seen.contains(canonical) {
                    seen.insert(canonical)
                    rows.append(csvEscape(post.nameString))
                }
            }
        }

        return rows
    }

    func postsCSVDedupedEmailRows(sortedThreads: [Thread]) -> [String] {
        var seen: Set<String> = []
        var rows: [String] = []
        rows.append(csvEscape("email_address"))

        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                let canonical = post.emailString.lowercased()
                if !seen.contains(canonical) {
                    seen.insert(canonical)
                    rows.append(csvEscape(canonical))
                }
            }
        }

        return rows
    }

    func postsCSVMalformedEmailRows(sortedThreads: [Thread]) -> [String] {
        var seen: Set<String> = []
        var rows: [String] = []
        rows.append(csvEscape("email_address"))

        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                let canonical = post.emailString.lowercased()
                if canonical.isEmpty || isValidEmail(canonical) {
                    continue
                }
                if !seen.contains(canonical) {
                    seen.insert(canonical)
                    rows.append(csvEscape(canonical))
                }
            }
        }

        return rows
    }

    func postsCSVCorrectEmailRows(sortedThreads: [Thread]) -> [String] {
        var seen: Set<String> = []
        var rows: [String] = []
        rows.append(csvEscape("email_address"))

        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            for post in sortedPosts {
                let canonical = post.emailString.lowercased()
                if canonical.isEmpty || !isValidEmail(canonical) {
                    continue
                }
                if !seen.contains(canonical) {
                    seen.insert(canonical)
                    rows.append(csvEscape(canonical))
                }
            }
        }

        return rows
    }

    func exportAnalysis(sortedThreads: [Thread], siteURL: URL) {
        var allPosts: [Post] = []
        for thread in sortedThreads {
            let sortedPosts = thread.posts.sorted { $0.createdAt < $1.createdAt }
            allPosts.append(contentsOf: sortedPosts)
        }

        if allPosts.isEmpty {
            let output = "No posts found.\n"
            let analysisURL = siteURL.appendingPathComponent("analysis.txt")
            try? output.write(to: analysisURL, atomically: true, encoding: .utf8)
            return
        }

        var unionFind = UnionFind(count: allPosts.count)
        var keyToIndex: [String: Int] = [:]

        for (index, post) in allPosts.enumerated() {
            let nameKey = normalizeKey(post.nameString)
            let emailKey = normalizeKey(post.emailString)
            for key in [nameKey, emailKey] where !key.isEmpty {
                if let existingIndex = keyToIndex[key] {
                    unionFind.union(index, existingIndex)
                } else {
                    keyToIndex[key] = index
                }
            }
        }

        var countsByRoot: [Int: Int] = [:]
        for index in allPosts.indices {
            let root = unionFind.find(index)
            countsByRoot[root, default: 0] += 1
        }

        let groupedCounts = countsByRoot.values.filter { $0 > 1 }.sorted(by: >)
        let usersWithMultiplePosts = groupedCounts.count
        let totalPostsInGroups = groupedCounts.reduce(0, +)

        var lines: [String] = []
        lines.append("Total posts: \(allPosts.count)")
        lines.append("Users with 2+ posts: \(usersWithMultiplePosts)")
        lines.append("Posts belonging to multi-post users: \(totalPostsInGroups)")
        lines.append("")
        lines.append("UserID\tPostCount")

        for (index, count) in groupedCounts.enumerated() {
            lines.append("User\(index + 1)\t\(count)")
        }

        let analysisURL = siteURL.appendingPathComponent("analysis.txt")
        do {
            try lines.joined(separator: "\n").write(to: analysisURL, atomically: true, encoding: .utf8)
            print("Analysis file successfully saved at \(analysisURL.path)")
        } catch {
            print("Failed to save analysis file: \(error)")
        }
    }

    func normalizeKey(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    struct UnionFind {
        private var parent: [Int]
        private var rank: [Int]

        init(count: Int) {
            parent = Array(0..<count)
            rank = Array(repeating: 0, count: count)
        }

        mutating func find(_ x: Int) -> Int {
            if parent[x] != x {
                parent[x] = find(parent[x])
            }
            return parent[x]
        }

        mutating func union(_ x: Int, _ y: Int) {
            let rootX = find(x)
            let rootY = find(y)
            if rootX == rootY {
                return
            }
            if rank[rootX] < rank[rootY] {
                parent[rootX] = rootY
            } else if rank[rootX] > rank[rootY] {
                parent[rootY] = rootX
            } else {
                parent[rootY] = rootX
                rank[rootX] += 1
            }
        }
    }

    func isValidEmail(_ value: String) -> Bool {
        let pattern = "^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$"
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return false
        }
        let range = NSRange(value.startIndex..<value.endIndex, in: value)
        return regex.firstMatch(in: value, options: [], range: range) != nil
    }
    
    func loadPosts(for threadID: Int, in directoryURL: URL, directoryName: String) throws -> [Post] {
        let threadURL = directoryURL.appendingPathComponent("\(threadID)")
        let postFiles = try FileManager.default.contentsOfDirectory(atPath: threadURL.path)
            .filter { $0.hasPrefix("post.") }
        
        var posts: [Post] = []
        
        for postFile in postFiles {
            let postFileURL = threadURL.appendingPathComponent(postFile)
            let content = try String(contentsOf: postFileURL, encoding: .isoLatin1)
            
            if let post = parsePostContent(postFile: postFile, content: content, directoryName: directoryName) {
                posts.append(post)
            }
        }
        
        return posts.sorted { $0.postID < $1.postID }
    }
    
    func parsePostContent(postFile: String, content: String, directoryName: String) -> Post? {
        guard let postID = Int(postFile.components(separatedBy: ".").last ?? "") else { return nil }

        // Extract the title line
        guard let titleStartRange = content.range(of: "<font color=\"#FF0000\">"),
              let titleEndRange = content.range(of: "</font><br>", range: titleStartRange.upperBound..<content.endIndex) else { return nil }

        let postTitle = content[titleStartRange.upperBound..<titleEndRange.lowerBound]
            .trimmingCharacters(in: .whitespacesAndNewlines)

        // Extract the date line
        guard let dateStartRange = content.range(of: "<tt>"),
              let dateEndRange = content.range(of: "</tt>", range: dateStartRange.upperBound..<content.endIndex) else { return nil }

        let dateString = content[dateStartRange.upperBound..<dateEndRange.lowerBound]
            .trimmingCharacters(in: .whitespacesAndNewlines)

        guard let createdAt = parseDate(from: dateString) else { return nil }

        // Extract the post body
        guard let bodyStartRange = content.range(of: "</tt><p>"),
              let bodyEndRange = content.range(of: "<p><code>--", range: bodyStartRange.upperBound..<content.endIndex) else { return nil }

        let postBody = content[bodyStartRange.upperBound..<bodyEndRange.lowerBound]
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "<br>", with: "\n")
            .replacingOccurrences(of: "&quot;", with: "\"")

        // Extract email and name
        guard let authorStartRange = content.range(of: "<p><code>--"),
              let emailNameEndRange = content.range(of: ") </code>", range: authorStartRange.upperBound..<content.endIndex),
              let emailNameString = content[authorStartRange.upperBound..<emailNameEndRange.lowerBound]
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .split(separator: "(", maxSplits: 1, omittingEmptySubsequences: true).map({ String($0).trimmingCharacters(in: .whitespaces) }) as [String]?,
              emailNameString.count == 2 else { return nil }

        let emailString = emailNameString[0]
        let nameString = emailNameString[1]

        return Post(postID: postID,
                    createdAt: createdAt,
                    postTitle: postTitle,
                    postBody: postBody,
                    emailString: emailString,
                    nameString: nameString,
                    directory: directoryName)
    }

    func parseDate(from dateString: String) -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "EEE MMM d HH:mm:ss VV yyyy"
        return formatter.date(from: dateString.trimmingCharacters(in: .whitespacesAndNewlines))
    }
    
    func exportThreadsToHTML() {
        var htmlContent = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Threads Export</title>
            <style>
                body {
                    font-family: Helvetica, Arial, sans-serif;
                    line-height: 1.5;
                    margin: 20px auto;
                    max-width: 800px;
                    padding: 20px;
                    background-color: #f8f8f8;
                    color: #333;
                }
                h2 {
                    color: #0077cc;
                    border-bottom: 2px solid #0077cc;
                    padding-bottom: 5px;
                }
                h3 {
                    margin-bottom: 5px;
                }
                .post {
                    background-color: #fff;
                    padding: 15px;
                    margin-bottom: 15px;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .post-meta {
                    font-size: 0.9em;
                    color: #666;
                    margin-bottom: 10px;
                }
                pre {
                    white-space: pre-wrap;
                    font-family: inherit;
                    margin-top: 0;
                }
            </style>
        </head>
        <body>
        """

        for thread in threads {
            htmlContent += "<h2>\(thread.indexID): \(thread.threadTitle)</h2>\n"
            
            let sortedPosts = thread.posts.sorted(by: { $0.createdAt < $1.createdAt })
            
            for post in sortedPosts {
                htmlContent += """
                <div class="post">
                    <h3>\(post.postTitle)</h3>
                    <div class="post-meta">By \(post.nameString) (\(post.emailString)) on \(formattedDate(post.createdAt))</div>
                    <pre>\(escapeHTML(post.postBody))</pre>
                </div>
                """
            }
        }

        htmlContent += """
        </body>
        </html>
        """

        let exportURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
            .appendingPathComponent("ThreadsExport.html")

        do {
            try htmlContent.write(to: exportURL, atomically: true, encoding: .utf8)
            print("HTML file successfully saved at: \(exportURL.path)")
        } catch {
            print("Failed to save HTML file: \(error)")
        }
    }
/*
    func formattedDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter.string(from: date)
    }

    func escapeHTML(_ text: String) -> String {
        text.replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\"", with: "&quot;")
            .replacingOccurrences(of: "'", with: "&#39;")
    }
 */
}
