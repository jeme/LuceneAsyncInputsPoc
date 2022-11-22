// See https://aka.ms/new-console-template for more information

using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Documents.Extensions;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using LuceneAsyncInputsPoc.CustomDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Directory = System.IO.Directory;

string root = Directory.CreateDirectory("temp-root").FullName;
foreach (string oldDir in Directory.GetDirectories(root))
    Directory.Delete(oldDir, true);

string indexDir = Directory.CreateDirectory(Path.Combine(root, "index")).FullName;
string snapshotDir = Directory.CreateDirectory(Path.Combine(root, "snapshot")).FullName;

Console.WriteLine($"Working in: {root}");

LuceneVersion version = LuceneVersion.LUCENE_48;
CustomSimpleFSDirectory directory = new(indexDir);
StandardAnalyzer analyzer = new(version);
IndexWriterConfig config = new(version, analyzer);
config.IndexDeletionPolicy = new SnapshotDeletionPolicy(config.IndexDeletionPolicy);

IndexWriter writer = new IndexWriter(directory, config);

void Write(string name, string surname, int age)
{
    string id = Guid.NewGuid().ToString("N");
    
    Document doc = new Document();
    doc.AddStringField("id", id, Field.Store.NO);
    doc.AddStringField("name", name, Field.Store.NO);
    doc.AddStringField("surname", surname, Field.Store.NO);
    doc.AddInt32Field("age", age, Field.Store.NO);
    doc.AddStoredField("$$RAW", JObject.FromObject(new { id, name, surname, age }).ToString(Formatting.None));
    writer.AddDocument(doc);
}

Write("Denzel", "Washington", 67);
Write("Thomas", "Hanks", 66);
Write("Christian", "Bale", 48);
Write("Morgan", "Freeman", 85);
Write("Thomas", "Cruise", 60);
Write("Keanu", "Reeves", 58);
Write("Hugh", "Jackman", 54);
Write("Ryan", "Reynolds", 46);
writer.Flush(false, true);
writer.Commit();

SnapshotDeletionPolicy sdp = (writer.Config.IndexDeletionPolicy as SnapshotDeletionPolicy)!;
IndexCommit commit = sdp.Snapshot();
CustomSimpleFSDirectory dir = (commit.Directory as CustomSimpleFSDirectory)!;
foreach (string fileName in commit.FileNames)
{
    IndexInput input = dir.OpenInput(fileName, IOContext.READ_ONCE);
    await using IndexInputStream streamWrapper = new IndexInputStream(input);
    await using FileStream output = File.Create(Path.Combine(snapshotDir, fileName));
    await streamWrapper.CopyToAsync(output);
}
sdp.Release(commit);

Write("Brianna", "Hildebrand", 26);
Write("Morena", "Baccarin", 43);
Write("Natalie", "Portman", 41);
writer.Flush(false, true);
writer.Commit();


IndexSearcher searcher = new(writer.GetReader(true));

TopDocs results = searcher.Search(new TermQuery(new Term("name", "Thomas")), 10);

Console.WriteLine($"Found: {results.TotalHits}");
foreach (ScoreDoc doc in results.ScoreDocs)
{
    Console.WriteLine(searcher.Doc(doc.Doc).GetField("$$RAW"));
}


//writer.GetReader(true).