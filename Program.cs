
using Parquet;
using Parquet.Data;
using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;


class Program
{
    static async Task Main()
    {
        string parquetPath = @"C:\Users\ELNAZER\Downloads\SUKARI_DYNAMIC\test_to_run_on_visualstudio\SUKARI_DYNAMIC_A_TSX_DVERT_20260310_32636_Xgknt3c4fA011V.parquet";
        string csvPath = @"C:\Users\ELNAZER\Downloads\SUKARI_DYNAMIC\test_to_run_on_visualstudio\output_chunks\output.csv";
        string outputFolder = @"C:\Users\ELNAZER\Downloads\SUKARI_DYNAMIC\test_to_run_on_visualstudio\output_chunks";

        await PrintParquetInfo(parquetPath);

        using var fs = File.OpenRead(parquetPath);
        using var reader = await ParquetReader.CreateAsync(fs);
          //2. Export to CSV
        await ExportToCsv(reader, fs, csvPath); // single file export
        await ExportToCsvChunked(reader, outputFolder);

        Console.WriteLine("\n Export completed!");
    }

    //  Main orchestrator
    static async Task PrintParquetInfo(string filePath)
    {
        using var fs = File.OpenRead(filePath);
        using var reader = await ParquetReader.CreateAsync(fs);

        var fields = reader.Schema.GetDataFields();

        PrintColumns(fields);
        PrintRowGroups(reader);

        long totalRows = CountRows(reader);
        Console.WriteLine($"\n Total Rows: {totalRows:N0}");
    }

    //  Print column info
    static void PrintColumns(dynamic fields)
    {
        Console.WriteLine("=== COLUMNS ===");

        foreach (var f in fields)
        {
            Console.WriteLine($"- {f.Name}");
        }

        Console.WriteLine($"\nTotal Columns: {fields.Length}");
        Console.WriteLine("----------------------------");
    }

    // Print row group details
    static void PrintRowGroups(ParquetReader reader)
    {
        Console.WriteLine("\n=== ROW GROUPS ===");

        for (int i = 0; i < reader.RowGroupCount; i++)
        {
            using var groupReader = reader.OpenRowGroupReader(i);

            Console.WriteLine($"RowGroup {i + 1}: {groupReader.RowCount:N0} rows");
        }
    }

    //  Count rows WITHOUT reopening file
    static long CountRows(ParquetReader reader)
    {
        long total = 0;

        for (int i = 0; i < reader.RowGroupCount; i++)
        {
            using var groupReader = reader.OpenRowGroupReader(i);
            total += groupReader.RowCount;
        }

        return total;
    }
    // =========================
    //  EXPORT METHOD
    // =========================
    static async Task ExportToCsvChunked(ParquetReader reader, string outputFolder)
    {
        const int chunkSize = 600_000;

        long totalRows = 0;
        long grandTotalRows = CountRows(reader);

        var sw = Stopwatch.StartNew();
        var fields = reader.Schema.GetDataFields();

        int fileIndex = 1;
        int rowsInCurrentFile = 0;

        StreamWriter writer = CreateNewWriter(outputFolder, fileIndex, fields);

        for (int g = 0; g < reader.RowGroupCount; g++)
        {
            using var groupReader = reader.OpenRowGroupReader(g);

            var columns = new Parquet.Data.DataColumn[fields.Length];

            for (int i = 0; i < fields.Length; i++)
                columns[i] = await groupReader.ReadColumnAsync(fields[i]);

            int rowCount = columns[0].Data.Length;

            for (int r = 0; r < rowCount; r++)
            {
                // Check if we need new file
                if (rowsInCurrentFile >= chunkSize)
                {
                    writer.Dispose();

                    fileIndex++;
                    rowsInCurrentFile = 0;

                    writer = CreateNewWriter(outputFolder, fileIndex, fields);
                }

                // Write row
                for (int c = 0; c < columns.Length; c++)
                {
                    var value = columns[c].Data.GetValue(r);
                    string text = value?.ToString() ?? "";

                    if (text.Contains(",") || text.Contains("\"") || text.Contains("\n"))
                        text = "\"" + text.Replace("\"", "\"\"") + "\"";

                    writer.Write(text);

                    if (c < columns.Length - 1)
                        writer.Write(",");
                }

                writer.WriteLine();

                totalRows++;
                rowsInCurrentFile++;

                // Progress
                if (totalRows % 100000 == 0)
                {
                    ShowProgress(totalRows, grandTotalRows, sw);
                }
            }
        }

        writer.Dispose();

        // Final progress
        ShowProgress(totalRows, grandTotalRows, sw);
        Console.WriteLine();

        Console.WriteLine($"Total Rows Exported: {totalRows:N0}");
        Console.WriteLine($"Files Created: {fileIndex}");
    }
    static StreamWriter CreateNewWriter(string folder, int index, Parquet.Schema.DataField[] fields)
    {
        string filePath = Path.Combine(folder, $"output_part_{index}.csv");

        // Clear progress line BEFORE writing new log
        if (!Console.IsOutputRedirected)
        {
            Console.Write("\r" + new string(' ', Console.WindowWidth - 1) + "\r");
        }
        Console.WriteLine($"\nCreating file: output_part_{index}.csv");

        var writer = new StreamWriter(filePath, false, Encoding.UTF8);

        // Write header
        for (int i = 0; i < fields.Length; i++)
        {
            writer.Write(fields[i].Name);
            if (i < fields.Length - 1) writer.Write(",");
        }
        writer.WriteLine();


        return writer;
    }
    static async Task ExportToCsv(ParquetReader reader, FileStream fs, string csvPath)
    {
        int totalRows = 0;
        long fileSize = fs.Length;
        var sw = Stopwatch.StartNew();

        long grandTotalRows = CountRows(reader);

        using var writer = new StreamWriter(csvPath, false, Encoding.UTF8);

        var fields = reader.Schema.GetDataFields();

        //  Write header
        for (int i = 0; i < fields.Length; i++)
        {
            writer.Write(fields[i].Name);
            if (i < fields.Length - 1) writer.Write(",");
        }
        writer.WriteLine();

        //  Process row groups
        for (int g = 0; g < reader.RowGroupCount; g++)
        {
            using var groupReader = reader.OpenRowGroupReader(g);




            var columns = new Parquet.Data.DataColumn[fields.Length];

            for (int i = 0; i < fields.Length; i++)
                columns[i] = await groupReader.ReadColumnAsync(fields[i]);

            int rowCount = columns[0].Data.Length;

            for (int r = 0; r < rowCount; r++)
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    var value = columns[c].Data.GetValue(r);
                    string text = value?.ToString() ?? "";

                    //  CSV escaping
                    if (text.Contains(",") || text.Contains("\"") || text.Contains("\n"))
                    {
                        text = "\"" + text.Replace("\"", "\"\"") + "\"";
                    }

                    writer.Write(text);

                    if (c < columns.Length - 1)
                        writer.Write(",");
                }

                writer.WriteLine();
                totalRows++;

                //  progress every 100k rows
                /*if (totalRows % 100000 == 0)
                {
                    Console.Write($"\rRows exported: {totalRows:N0}");
                }*/
                //  Update progress every 100k rows
                if (totalRows % 100000 == 0)
                {
                    //ShowProgress(fs.Position, fileSize, totalRows, sw);
                    ShowProgress(totalRows, grandTotalRows, sw);

                }
            }
        }
        // Final update
        //ShowProgress(fileSize, fileSize, totalRows, sw);
        ShowProgress(totalRows, grandTotalRows, sw);
        Console.WriteLine(); // move to next line after progress
        Console.WriteLine($"\rRows exported: {totalRows:N0}");
    }
    static void ShowProgress(long rows, long totalRows, Stopwatch sw)
    {
        double percent = (double)rows / totalRows;

        int barWidth = 30;
        int filled = (int)(percent * barWidth);

        string bar = new string('█', filled) + new string('░', barWidth - filled);

        double speed = rows / Math.Max(sw.Elapsed.TotalSeconds, 1);
        TimeSpan eta = TimeSpan.FromSeconds((totalRows - rows) / Math.Max(speed, 1));
        TimeSpan elapsed = sw.Elapsed;

        string message =
            $"{bar} {percent * 100:0.0}% | Rows: {rows:N0} | Speed: {speed:N0}/s | ETA: {eta:mm\\:ss} | Elapsed: {elapsed:hh\\:mm\\:ss}";

        Console.Write("\r" + message);
    }


}