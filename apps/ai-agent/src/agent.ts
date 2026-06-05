
import { Kafka } from 'kafkajs';
import { IKafkaErrorEvent } from '@ai-debugger/shared-types';
import { GoogleGenerativeAI } from "@google/generative-ai";
import { Pinecone } from '@pinecone-database/pinecone';
import fs from 'fs';
import * as dotenv from 'dotenv';
import { simpleGit } from 'simple-git';
const git = simpleGit(); // GitOps Controller

// 1. Load environment variables
dotenv.config();

// --- DISCORD WEBHOOK SETUP ---
const DISCORD_WEBHOOK_URL =process.env.DISCORD_WEBHOOK_URL;



async function sendDiscordAlert(serviceName: string, errorMessage: string, rca: string, branchName: string) {
    try {
        const payload = {
            username: "AI SRE Agent",
            avatar_url: "https://cdn-icons-png.flaticon.com/512/4712/4712035.png", // Robot icon
            embeds: [
                {
                    title: `🚨 System Auto-Healed: ${serviceName}`,
                    color: 16711680, // Red color for alerts
                    description: "An exception was detected in production. The AI Agent has analyzed the issue, generated a fix, and raised a PR.",
                    fields: [
                        { name: "🔴 Error Message", value: `\`${errorMessage}\``, inline: false },
                        { name: "🧠 Root Cause Analysis", value: rca.substring(0, 1000), inline: false }, // Truncated to avoid Discord limits
                        { name: "🌿 Git Branch (PR Ready)", value: `\`${branchName}\``, inline: false }
                    ],
                    footer: { text: "Event-Driven AI Observability Pipeline" },
                    timestamp: new Date().toISOString()
                }
            ]
        };

        await fetch(DISCORD_WEBHOOK_URL as string, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        console.log("📲 [DISCORD] Alert successfully pushed to channel!");
    } catch (error) {
        console.error("❌ [DISCORD] Failed to send alert:", error);
    }
}


async function createPullRequest(branchName: string, serviceName: string, errorMsg: string, rca: string) {
    const githubToken = process.env.GITHUB_TOKEN;
    const githubRepo = process.env.GITHUB_REPO; // format: "username/repository-name"

    if (!githubToken || !githubRepo) {
        console.log("⚠️ GITHUB_TOKEN or GITHUB_REPO missing. Skipping automatic PR creation.");
        return;
    }

    try {
        const response = await fetch(`https://api.github.com/repos/${githubRepo}/pulls`, {
            method: 'POST',
            headers: {
                'Authorization': `token ${githubToken}`,
                'Accept': 'application/vnd.github.v3+json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                title: `🚨 AI Auto-Fix: Exception in ${serviceName}`,
                body: `### AI Generated Fix\n**Error:** ${errorMsg}\n\n**RCA:**\n${rca}`,
                head: branchName,
                base: 'main' // Aapki default branch (main ya master)
            })
        });

        if (response.ok) {
            const data = await response.json();
            console.log(`✅ [GITHUB] Pull Request created successfully: ${data.html_url}`);
        } else {
            const errorData = await response.json();
            console.error("❌ [GITHUB] Failed to create PR:", errorData.message);
        }
    } catch (error) {
        console.error("❌ [GITHUB] API Error:", error);
    }
}

// (Baki saare imports waise hi rahenge)

// --- MCP CONCEPT: Local File System Access Helper ---
// function extractCodeContext(stackTrace: string) {
//     // Regex to find file path and line number in the stack trace
//     const match = stackTrace.match(/(\/.*?\.ts):(\d+)/);
    
//     if (match) {
//         const filePath = match[1];
//         const errorLine = parseInt(match[2], 10);
//         try {
//             console.log(`📂 [MCP] Reading FULL local file: ${filePath}`);
//             const fullFileContent = fs.readFileSync(filePath, 'utf-8');
            
//             // Ab hum poori file return kar rahe hain, sirf 11 lines nahi!
//             return { filePath, fullContent: fullFileContent, errorLine };
//         } catch (e) {
//             console.log("⚠️ [MCP] Could not read local file for context.");
//             return null;
//         }
//     }
//     return null;
// }

function extractCodeContext(stackTrace: string) {
    const match = stackTrace.match(/(\/.*?\.ts):(\d+)/);
    
    if (match) {
        const filePath = match[1];
        const errorLine = parseInt(match[2], 10);
        try {
            console.log(`📂 [MCP] Reading local file: ${filePath}`);
            const fullContent = fs.readFileSync(filePath, 'utf-8');
            
            // File ko lines mein break karo
            const lines = fullContent.split('\n');
            
            // Error line se 15 line upar aur 15 line neeche ka data uthao (Total ~30 lines)
            const startLine = Math.max(0, errorLine - 15);
            const endLine = Math.min(lines.length, errorLine + 15);
            
            const snippet = lines.slice(startLine, endLine).join('\n');
            
            return { filePath, fullContent, snippet, errorLine };
        } catch (e) {
            console.log("⚠️ [MCP] Could not read local file for context.");
            return null;
        }
    }
    return null;
}

const apiKey = process.env.GEMINI_API_KEY;
const pineconeApiKey = process.env.PINECONE_API_KEY;

if (!apiKey || !pineconeApiKey) {
  throw new Error("Missing GEMINI_API_KEY or PINECONE_API_KEY in .env");
}

// 2. Initialize Gemini (Text & Embedding Models)
const genAI = new GoogleGenerativeAI(apiKey);

// For generating RCA
const aiModel = genAI.getGenerativeModel({
    model: "gemini-3-flash-preview", // Yahan apna 'gemini-3-flash-preview' rakh sakte ho
    generationConfig: { maxOutputTokens: 3048, temperature: 0.1 },
});

// For converting text to Vectors (Industry standard is text-embedding-004)
const embeddingModel = genAI.getGenerativeModel({ model: "embedding-001" });

// 3. Initialize Pinecone Vector DB
const pc = new Pinecone({ apiKey: pineconeApiKey });
const index = pc.index(process.env.PINECONE_INDEX || 'ai-debugger-memory');

// 4. Kafka Setup
const kafka = new Kafka({ clientId: 'ai-debugger-agent', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'ai-debugger-group' });

// Helper: Text ko Embeddings (768 dimensions) me convert karna
// async function getEmbedding(text: string) {
//     const result = await embeddingModel.embedContent(text);
//     return result.embedding.values;
// }
// Helper: Text ko Embeddings (768 dimensions) me convert karna
async function getEmbedding(text: string) {
    try {
        // Attempt 1: Try Google's standard embedding model
        const result = await embeddingModel.embedContent(text);
        return result.embedding.values;
    } catch (apiError) {
        console.log("⚠️ Google Embedding API failed. Activating Local Fallback Vector...");
        
        // Attempt 2 (The Architect Hack): Agar Google API fail ho jaye, 
        // toh text ke characters se khud ek 768-dimension ka deterministic array bana lo.
        // Isse same error aane par hamesha exactly same vector banega aur Pinecone match kar lega!
        const vector = new Array(768).fill(0.01); 
        for (let i = 0; i < text.length; i++) {
            // Use char code to generate a unique but consistent mathematical number
            vector[i % 768] = (text.charCodeAt(i) % 100) / 100;
        }
        return vector;
    }
}

const startAgent = async () => {
    try {
        await consumer.connect();
        console.log("🤖 [AI AGENT] Connected to Kafka Consumer");
        await consumer.subscribe({ topic: 'global-error-stream', fromBeginning: false });
        console.log("🎧 Listening for new microservice errors...\n");

       await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (!message.value) return;
                
                // 🛑 SAFETY NET: Try-catch block to prevent Kafka infinite loops
                try {
                    const errorEvent: IKafkaErrorEvent = JSON.parse(message.value.toString());
                    console.log(`\n🚨 [NEW INCIDENT DETECTED] : ${errorEvent.serviceName}`);
                    console.log(`📝 Message: ${errorEvent.errorDetails.message}`);
                    console.log("-------------------------------------------------");

                    // STEP A: Vector Search (Retrieval)
                    const errorSignature = `Error in ${errorEvent.serviceName}: ${errorEvent.errorDetails.message}`;
                    console.log("🔍 Searching Pinecone Vector DB for historical context...");
                    
                    const vector = await getEmbedding(errorSignature);
                    const queryResponse = await index.query({
                        vector: vector,
                        topK: 1, 
                        includeMetadata: true
                    });

                    let historicalContext = "";
                    if (queryResponse.matches.length > 0 && queryResponse.matches[0].score && queryResponse.matches[0].score > 0.8) {
                        console.log("🟢 [MEMORY HIT] Similar past incident found!");
                        historicalContext = `
                        PAST SIMILAR INCIDENT:
                        Error: ${queryResponse.matches[0].metadata?.errorMsg}
                        Past Solution: ${queryResponse.matches[0].metadata?.rca}
                        `;
                    } else {
                        console.log("🔴 [MEMORY MISS] No exact history found. Generating fresh RCA...");
                    }

                    // STEP B: Context-Aware Generation (Augment & Generate)
              console.log("🧠 Analyzing with Gemini AI...");
const localCode = extractCodeContext(errorEvent.errorDetails.stack);

const codeBlock = localCode ? `
ERROR LINE CONTEXT (30 lines around the error in ${localCode.filePath}):
\`\`\`typescript
${localCode.snippet}
\`\`\`
` : "Code context not available.";

const prompt = `
You are an Expert Site Reliability Engineer (SRE). 
Analyze the following error details.

CURRENT INCIDENT:
Service Name: ${errorEvent.serviceName}
Error Message: ${errorEvent.errorDetails.message}

${codeBlock}

Format your response in plain text with two headings: 
1. Root Cause
2. Code Fix 
CRITICAL: Under "Code Fix", provide ONLY the updated version of the provided 30-line code snippet wrapped in \`\`\`typescript ... \`\`\`. Do NOT provide the entire file. We will replace the old snippet with your new snippet.
`;

const result = await aiModel.generateContent(prompt);
const rcaText = result.response.text();

console.log("\n💡 [AI ROOT CAUSE ANALYSIS] 💡");
console.log("=================================================\n");

if (localCode) {
    const codeMatch = rcaText.match(/```typescript\n([\s\S]*?)```/);
    
    if (codeMatch && codeMatch[1]) {
        const newSnippetContent = codeMatch[1].trim();
        const branchName = `auto-fix/${errorEvent.serviceName}-${Date.now()}`;
        
        console.log(`\n🛠️ [GITOPS] Auto-Healing Initiated for ${localCode.filePath}...`);
        
        try {
            // 1. Sirf purane snippet ko naye snippet se replace karo (Baaki file safe rahegi)
            const newFileContent = localCode.fullContent.replace(localCode.snippet, newSnippetContent);
            fs.writeFileSync(localCode.filePath, newFileContent);
            console.log(`✏️ Source code successfully patched.`);

            // 2. Git Branch, Commit, aur Push
            await git.checkoutLocalBranch(branchName);
            await git.add(localCode.filePath);
            await git.commit(`fix(${errorEvent.serviceName}): Auto-healed bug\n\nAI RCA: ${errorEvent.errorDetails.message}`);
            
            console.log(`🌿 Git branch '${branchName}' created & committed!`);
            
            // Automatically push to remote
            await git.push('origin', branchName);
            console.log(`☁️ Code pushed to remote repository.`);

            // 3. GitHub PR Create karna
            await createPullRequest(branchName, errorEvent.serviceName, errorEvent.errorDetails.message, rcaText);
            
            // Agent ko wapas main branch pe laana
            await git.checkout('main'); 
            await sendDiscordAlert(errorEvent.serviceName, errorEvent.errorDetails.message, rcaText, branchName);

        } catch (gitError) {
            console.error("❌ [GITOPS] Failed to apply git commit or push:", gitError);
        }
    }
}

                    // STEP C: Save back to Vector DB
                    console.log("💾 Memorizing this RCA to Vector Database...");
                   await index.upsert({
                        records: [
                            {
                                id: `evt_${Date.now()}`,
                                values: vector,
                                metadata: {
                                    service: errorEvent.serviceName,
                                    errorMsg: errorEvent.errorDetails.message,
                                    rca: rcaText
                                }
                            }
                        ]
                    });
                    console.log("✅ Incident successfully added to Long-Term Memory!");

                } catch (err) {
                    // Agar koi error aata hai (like API down), hum loop break kar denge
                    console.error("⚠️ Skipping poison message to prevent infinite loop:", err);
                }
            },
        });
    } catch (error) {
        console.error("Failed to start AI Agent:", error);
    }
};

startAgent();
























