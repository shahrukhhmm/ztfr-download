require("dotenv").config();

const express = require("express");

const { S3, GetObjectCommand } = require("@aws-sdk/client-s3");

const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { Readable } = require("stream");
const archiver = require("archiver");
const app = express();
const mysql = require("mysql2");
const bodyParser = require("body-parser");
const router = express.Router();
const path = require("path");
const Hashids = require("hashids");
const cors = require("cors");
const hashids = new Hashids();
const { Buffer } = require("buffer");

// const { pipeline } = require("stream/promises");
const { promisify } = require("util");
const pipeline = promisify(require("stream").pipeline);
// const pipelineAsync = promisify(pipeline);

app.use(express.static(path.join(__dirname, "public")));

router.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send("Something went wrong!");
});

const s3 = new S3({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const s3Bucket = process.env.AWS_BUCKET_NAME;

// MySQL database connection
const db = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  connectTimeout: 20000,
});

db.connect((err) => {
  if (err) {
    console.error("Error connecting to MySQL database: " + err.stack);
    return;
  }
  console.log("Connected to MySQL database as id " + db.threadId);
});

router.get("/api/download", async (req, res) => {
  try {
    const id = req.query.key;

    if (!id) {
      return res.status(400).send("Invalid fileId");
    }

    const fileId = Buffer.from(id, "base64").toString("utf-8");

    const query = "SELECT * FROM file_details WHERE id = ?";

    const [results] = await db.promise().query(query, [fileId]);

    if (results.length === 0) {
      return res.status(404).send("File not found");
    }

    const file = results[0];
    const s3Key = file.file_key;
    const unit = file.unit;

    const filename = s3Key.substring(s3Key.lastIndexOf("/") + 1);

    const params = {
      Bucket: s3Bucket,
      Key: s3Key,
    };

    const response = await s3.send(new GetObjectCommand(params));

    const s3Stream = Readable.from(response.Body);

    // Calculate total file size
    const fileSize = parseFloat(file.file_size);
    const totalSizeBytes = Math.abs(convertToBytes(fileSize, unit));

    // Set response headers
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.setHeader("Content-Type", "application/octet-stream");
    res.setHeader(
      "Content-Range",
      `bytes 0-${totalSizeBytes - 1}/${totalSizeBytes}`
    );
    res.setHeader("Content-Length", totalSizeBytes);
    res.setHeader("Accept-Ranges", "bytes");

    // Send initial headers
    res.status(200);

    let downloadedSize = 0;
    let startTime = Date.now();

    // Handle the 'error' event first to catch any errors during the setup
    s3Stream.on("error", (err) => {
      console.error("S3 Stream error:", err);
      if (!res.headersSent) {
        res.status(500).send(err.message || "Internal Server Error");
      }
    });

    // Pipe the S3 stream to the response stream
    s3Stream
      .on("data", (chunk) => {
        downloadedSize += chunk.length;

        const currentTime = Date.now();
        const elapsedTime = (currentTime - startTime) / 1000; // seconds
        const speed = downloadedSize / elapsedTime / 1024; // KB/s

        // Log or send the progress information to the client as needed
        const formattedDownloadedSize = formatBytes(downloadedSize);
        const formattedTotalSize = formatBytes(totalSizeBytes);
        // console.log(
        //   `${filename}\nhttp://127.0.0.1:5000\n${speed.toFixed(
        //     2
        //   )} KB/s - ${formattedDownloadedSize} of ${formattedTotalSize}, ${(
        //     (totalSizeBytes - downloadedSize) /
        //     speed /
        //     3600
        //   ).toFixed(2)} hours left`
        // );

        // Check if headers have been sent before updating
        if (!res.headersSent) {
          res.setHeader(
            "Content-Range",
            `bytes ${downloadedSize}-${totalSizeBytes - 1}/${totalSizeBytes}`
          );
        }

        // Check if the response is still writable before updating headers or writing chunks
        if (!res.writableEnded) {
          res.write(chunk);
        }
      })
      .on("end", () => {
        // S3 stream ended, close the response stream
        res.end();
        console.log("Download completed for fileId:", fileId);
      });
  } catch (err) {
    console.error("MySQL Query Error:", err);
    // Log additional details
    console.error("Error Code:", err.code);
    console.error("SQL State:", err.sqlState);
    console.error("Error Message:", err.sqlMessage);

    if (!res.headersSent) {
      res.status(500).send(err.message || "Internal Server Error");
    }
  }
});

function convertToBytes(size, unit) {
  const unitMap = {
    B: 1,
    KB: 1024,
    MB: 1024 * 1024,
    GB: 1024 * 1024 * 1024,
    TB: 1024 * 1024 * 1024 * 1024,
  };

  return size * unitMap[unit.toUpperCase()];
}

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return "0 Bytes";

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

app.use(bodyParser.json());
app.use(cors());
app.use("/", router);

process.on("SIGTERM", () => {
  db.end();
  server.close(() => {
    console.log("Server closed");
  });
});

app.use("*", (req, res) => {
  res.status(404).sendFile(__dirname + "/404.html");
});

const port = process.env.PORT || 5000;

const server = app.listen(port, () => {
  server.timeout = 0;
  console.log(`Server is running on port ${port}`);
});
