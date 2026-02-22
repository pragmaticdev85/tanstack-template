import * as fs from 'node:fs';
import csvParser from 'csv-parser';
import { spawn } from 'node:child_process'
import sqlite3 from 'sqlite3';
const db = new sqlite3.Database('./yt.sqlite');

// Define the type for your CSV data
type YTEntry = {
    sno: number,
    url: string
};

interface YTStatus {
    id?: number; // Optional, as SQLite will auto-increment
    sno: number,
    url: string;
    status: string;
}

const results: YTEntry[] = [];
const filePath: string = './public/pl.csv';

const ytDownload = async (entryId: number, videoUrl: string) => {
    const log = (level: 'info' | 'debug' | 'error', message: string) => {
        switch (level) {
            case 'debug':
                console.debug(`[Entry-${entryId}] ${message}`);
                break;
            case 'info':
                console.info(`[Entry-${entryId}] ${message}`);
                break;
            case 'error':
                console.error(`[Entry-${entryId}] ${message}`);
                break;
        }
    };
    if (!videoUrl || videoUrl.length == 0) return;
    // Define the URL and yt-dlp arguments
    const args = ['-f', 'bestvideo+bestaudio', '-o', './downloads/%(title)s.%(ext)s', videoUrl];
    await new Promise((resolve, reject) => {
        // Spawn the process (ensure 'yt-dlp' is in your system PATH)
        const ytdlp = spawn('yt-dlp', args);

        // Capture standard output (stdout)
        ytdlp.stdout.on('data', (data) => {
            log('debug', `${data}`);
        });

        // Capture errors or progress messages (stderr)
        ytdlp.stderr.on('data', (data) => {
            log('error', `${data}`);
        });

        // Listen for the 'close' event to know when the process is finished
        ytdlp.on('close', (code) => {
            const db = new sqlite3.Database('./yt.sqlite')
            if (code == 0) {
                db.run('UPDATE yt_status SET status=? WHERE id=?', ['success', entryId], function (err) {
                    if (err) log('error', `Unable to update the success status with the entry ${entryId}; error: ${err}`)
                });
                log('info', `Resource '${entryId} - ${videoUrl}' is successfully downloaded`);
                db.close((err) => {
                    if (err) {
                        console.error(err.message);
                    }
                    console.log('Closed the database connection.');
                });
                resolve('SUCCESS');
            } else {
                db.run('DELETE FROM yt_status WHERE id=?', [entryId], function (err) {
                    log('error', `Unable to delete the the entry ${entryId}; error: ${err}`)
                });
                db.close((err) => {
                    if (err) {
                        console.error(err.message);
                    }
                    console.log('Closed the database connection.');
                });
                reject('FAILED')
            }
            
        });
    });
};

(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS yt_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sno INTEGER NOT NULL UNIQUE,
            url TEXT NOT NULL,
            status TEXT NOT NULL
        )
    `, (err) => {
        if (err) {
            console.error(err.message);
            return;
        }
        console.log('yt_status table ensured.');
    });
    fs.createReadStream(filePath)
        .pipe(csvParser())
        .on('data', (row: YTEntry) => {
            // Process each row as it is read
            results.push(row);
        })
        .on('end', async () => {
            // The 'end' event is fired when the entire file has been processed
            console.log('CSV file successfully processed.');
            await Promise.all(results.map((value) => {
                const ytStatus: YTStatus = {
                    sno: value.sno,
                    url: value.url,
                    status: 'pending'
                };
                const insertSql = `INSERT INTO yt_status (sno, url, status) VALUES (?, ?, ?)`;
                db.run(insertSql, [ytStatus.sno, ytStatus.url, ytStatus.status], async function (err) {
                    if (err?.message) {
                        if (err?.message.indexOf('SQLITE_CONSTRAINT') > -1) {
                            console.log(`[Entry-${value.sno}] already present with URL: ${ytStatus.url}; so skipping`);
                        } else {
                            console.error(err.message);
                        }
                        return;
                    }
                    // 'this' refers to the statement object in the callback, giving you the last inserted ID
                    console.log(`A row has been inserted with rowid ${this.lastID}`);
                    console.log(`Downloading the media ${value.url}`);
                    await ytDownload(this.lastID, value.url);
                });
            }));

            db.close((err) => {
                if (err) {
                    console.error(err.message);
                }
                console.log('Closed the database connection.');
            });
            // All subsequent logic that depends on the complete data should go here or be called from here
        })
        .on('error', (error: Error) => {
            console.error('An error occurred:', error.message);
        });
})();

