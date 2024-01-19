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

// AWS credentials

// AWS.config.update({
//   accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//   secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
//   region: process.env.AWS_REGION,

//   httpOptions: {
//     timeout: 0,
//   },
// });

// const s3 = new S3({

//   maxAttempts: 1000000,

//   retryDelayOptions: { base: 10000 },

//   credentials: {
//     accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//     secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
//   },

//   region: process.env.AWS_REGION,
//   httpOptions: {
//     timeout: 0,
//   },
// });

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

    // const s3Stream = s3.getObject(params).createReadStream();

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
        const elapsedTime = (currentTime - startTime) / 1000; // in seconds
        const speed = downloadedSize / elapsedTime / 1024; // in KB/s

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

// shorten url

router.post("/api/shorten", async (req, res) => {
  const originalUrl = req.body.url;

  const generateRandomNumber = () => {
    const min = 100000000000;
    const max = 999999999999;
    return Math.floor(Math.random() * (max - min + 1)) + min;
  };

  const hashids = new Hashids("refsnartz", 12);

  const randomNumber = generateRandomNumber();
  const code = hashids.encode(randomNumber);

  // res.send(code);
  try {
    const query =
      "INSERT INTO shortened_urls (original_url, shortened_code, created_at) VALUES (?, ?, ?)";

    await db.promise().query(query, [originalUrl, code, new Date()]);

    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const shortenedUrl = baseUrl + "/" + code;

    res.json({
      shortened_url: shortenedUrl,
      error: false,
      message: "Short code saved successfully.",
    });
  } catch (error) {
    console.error("Error inserting data:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error.",
    });
  }
});

// redirect to the original url
router.get("/:code", async (req, res) => {
  const shortenedCode = req.params.code;

  try {
    const query =
      "SELECT original_url FROM shortened_urls WHERE shortened_code = ?";
    const [results] = await db.promise().query(query, [shortenedCode]);

    if (results.length === 0) {
      return res.status(404).json({
        error: true,
        message: "Shortened URL not found.",
      });
    }

    const originalUrl = results[0].original_url;

    res.redirect(originalUrl);
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error.",
    });
  }
});

async function addFileToArchive(fileDetailId, archive, processedFolders) {
  try {
    const query = "SELECT * FROM file_details WHERE id = ?";
    const [results] = await db.promise().query(query, [fileDetailId]);

    if (results.length === 0) {
      console.error("File not found for file_detail_id: " + fileDetailId);
      return;
    }

    const file = results[0];
    const s3Key = file.file_key;
    const folderName = file.folder_name || "";

    if (!folderName) {
      const lastSlashIndex = s3Key.lastIndexOf("/");
      const filename = s3Key.substring(lastSlashIndex + 1);
      const archiveEntryName = filename;

      const params = {
        Bucket: s3Bucket,
        Key: s3Key,
      };

      const s3Stream = s3.getObject(params).createReadStream();
      archive.append(s3Stream, {
        name: archiveEntryName,
      });

      return new Promise((resolve, reject) => {
        s3Stream.on("end", resolve);
        s3Stream.on("error", reject);
      });
    } else {
      const relativeFilePath = folderName + "/" + s3Key.split("/").pop();

      if (!processedFolders.includes(folderName)) {
        archive.append(null, { name: `${folderName}/` });
        processedFolders.push(folderName);
      }

      const params = {
        Bucket: s3Bucket,
        Key: s3Key,
      };

      const s3Stream = s3.getObject(params).createReadStream();
      archive.append(s3Stream, {
        name: relativeFilePath,
      });

      return new Promise((resolve, reject) => {
        s3Stream.on("end", resolve);
        s3Stream.on("error", (err) => {
          console.error("Error fetching S3 object:", err);
          resolve();
        });
      });
    }
  } catch (err) {
    console.error("Error:", err);
    // throw err;
  }
}

async function waitForStreams(streamPromises) {
  try {
    await Promise.all(streamPromises);
  } catch (err) {
    console.error("Error waiting for streams:", err);
    // throw err;
  }
}

router.get("/api/download-multiple", async (req, res) => {
  try {
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="multiple_files.zip"'
    );
    res.set("Content-Type", "application/zip");

    const archive = archiver("zip", {
      zlib: { level: 9 },
    });

    archive.pipe(res);

    const fileDetailIds = req.query.fileDetails || [];
    const processedFolders = [];

    const streamPromises = fileDetailIds.map((fileDetailId) =>
      addFileToArchive(fileDetailId, archive, processedFolders)
    );

    await waitForStreams(streamPromises);
    archive.finalize();
  } catch (err) {
    console.error("MySQL Query Error:", err);
    // Log additional details
    console.error("Error Code:", err.code);
    console.error("SQL State:", err.sqlState);
    console.error("Error Message:", err.sqlMessage);

    return res.status(500).send(err.message || "Internal Server Error");
  }
});

router.get("/api/downloadTeam", async (req, res) => {
  try {
    const fileIdWithBucketUrl = req.query.url;

    if (!fileIdWithBucketUrl) {
      return res.status(400).send("Invalid fileId");
    }

    // Remove the S3 bucket URL from fileId
    const fileId = fileIdWithBucketUrl.replace(
      "https://ztfr.s3.eu-west-2.amazonaws.com/",
      ""
    );

    const s3Key = fileId;

    const filename = s3Key.substring(s3Key.lastIndexOf("/") + 1);

    const params = {
      Bucket: s3Bucket,
      Key: s3Key,
    };

    const s3Stream = s3.getObject(params).createReadStream();

    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.set("Content-Type", "application/octet-stream");

    s3Stream.pipe(res);

    s3Stream.on("end", () => {
      console.log("Download completed for fileId:", fileId);
    });

    s3Stream.on("error", (err) => {
      if (res.headersSent) {
        console.error(
          "Error fetching S3 object, but headers already sent:",
          err
        );
      } else {
        if (err.code === "NoSuchKey") {
          console.error("Error fetching S3 object (NoSuchKey):", err);
          res.status(404).send("No such key found");
        } else {
          console.error("Error fetching S3 object:", err);
          res.status(500).send("Error fetching the S3 object");
        }
      }
    });
  } catch (err) {
    console.error("MySQL Query Error:", err);
    console.error("Error Code:", err.code);
    console.error("SQL State:", err.sqlState);
    console.error("Error Message:", err.sqlMessage);

    return res.status(500).send(err.message || "Internal Server Error");
  }
});

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
