using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


using System.Threading;

using LevelDB;
using LevelDB.NativePointer;

namespace LevelDBDemo
{
    class Program
    {
        static string testPath = @"C:\Temp\Test";
        static string CleanTestDB()
        {
            DB.Destroy(new Options { CreateIfMissing = true }, testPath);
            return testPath;
        }
        static void Main(string[] args)
        {
            var l = new Logger(s => Console.WriteLine(s));
            var x = new Options
            {
                CreateIfMissing = true,
                RestartInterval = 13,
                MaxOpenFiles = 100,
                InfoLog = l,

                BlockSize = 8 * 1024,
                CompressionLevel = CompressionLevel.SnappyCompression //CompressionLevel.NoCompression

            };

            var db = new DB(x, testPath);

            db.Put("Tampa", "green");
            db.Put("London", "red");
            db.Put("New York", "blue");

            Console.WriteLine(db.Get("Tampa"));
            Console.WriteLine(db.Get("London"));
            Console.WriteLine(db.Get("New York"));

            db.Delete("New York");

            Console.WriteLine(db.Get("New York"));

            db.Delete("New York");

            for (var j = 0; j < 5; j++)
            {
                var r = new Random(0);
                var data = "";

                for (int i = 0; i < 1024; i++)
                {
                    data += 'a' + r.Next(26);
                }
                for (int i = 0; i < 5 * 1024; i++)
                {
                    db.Put(string.Format("row{0}", i), data);
                }
                Thread.Sleep(100);
            }

            db.Dispose();
            GC.KeepAlive(l);
        }

        static public int fun(int x)
        {
            return x;
        }
        static public void TestCompare()
        {
            var path = CleanTestDB();

            var options = new Options { CreateIfMissing = true };
            options.Comparator = Comparator.Create(
                "integers mod 2",
                (xs, ys) => LexicographicalCompare(((NativeArray<int>)xs).Select(x => fun(x)),
                                                   ((NativeArray<int>)ys).Select(y => fun(y))));

            object[] o = { 1, "1", "2" };
            string s0 = o.ToString();
            string s01 = string.Join(",", o);
            object[] o1 = { "1,1", "2", "2" };
            object[] o2 = { "1", "1,2", "2" };
            string s1 = o1.ToString();
            string s2 = o2.ToString();
            string s11 = string.Join(",", o1);
            string s21 = string.Join(",", o2);

            using (var db = new DB(options, path))
            {

                byte[] tt = new byte[10];
                byte[] t = BitConverter.GetBytes(1234);

                db.Put(BitConverter.GetBytes(1234), System.Text.Encoding.Default.GetBytes("this is a test string."));

                db.Put(BitConverter.GetBytes(123), BitConverter.GetBytes(1234));

                byte[] ret = db.Get(BitConverter.GetBytes(123));
                byte[] ret2 = db.Get(BitConverter.GetBytes(1234));
                int i1 = BitConverter.ToInt32(ret, 0);
                string str1 = System.Text.Encoding.ASCII.GetString(ret2);
                db.Put(1, new[] { 1, 2, 3 }, new WriteOptions());
                db.Put(3, new[] { 10, 20, 30 }, new WriteOptions());
                Console.WriteLine("put 1, [1,2,3]");
                Console.WriteLine("get 1:" + db.Get(1));
                Console.WriteLine("get 3:" + db.Get(3));
                Console.WriteLine("get 5:" + db.Get(5));

                var key = NativeArray.FromArray(new int[] { 3 });
                using (var xs = db.GetRaw<int>(key))
                {
                    int[] arr = xs.ToArray();
                    string s = string.Format("get {0} => [{1}]", key[0], string.Join(",", xs));
                    Console.WriteLine("get {0} => [{1}]", key[0], string.Join(",", xs));

                }
            }
        }
        static private int LexicographicalCompare<T>(IEnumerable<T> xs, IEnumerable<T> ys)
        {
            var comparator = System.Collections.Generic.Comparer<T>.Default;

            using (var xe = xs.GetEnumerator())
            using (var ye = ys.GetEnumerator())
            {
                for (; ; )
                {
                    var xh = xe.MoveNext();
                    var yh = ye.MoveNext();
                    if (xh != yh)
                        return yh ? -1 : 1;
                    if (!xh)
                        return 0;
                    // more elements
                    int diff = comparator.Compare(xe.Current, ye.Current);
                    if (diff != 0)
                        return diff;
                }
            }
        }
    }
}
