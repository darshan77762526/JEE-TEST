import express from "express";
import cors from "cors";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { existsSync, writeFileSync, unlinkSync, mkdirSync, readFileSync } from "fs";
import { execSync } from "child_process";
import { randomUUID } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

/* ══════════════════════════════════════════
   API KEY ROTATION MANAGER
══════════════════════════════════════════ */
const apiKeyManager = (() => {
  function loadKeys() {
    const keys = [];
    if (process.env.GEMINI_API_KEY_POOL) {
      keys.push(...process.env.GEMINI_API_KEY_POOL.split(",").map(k => k.trim()).filter(Boolean));
    }
    const numbered = [
      process.env.GEMINI_API_KEY,  process.env.GEMINI_API_KEY_2,
      process.env.GEMINI_API_KEY_3,process.env.GEMINI_API_KEY_4,
      process.env.GEMINI_API_KEY_5,
    ].filter(Boolean);
    for (const k of numbered) { if (!keys.includes(k)) keys.push(k); }
    return keys;
  }
  const keys = loadKeys();
  let currentIndex = 0;
  const quotaExhaustedUntil = {};
  function getCurrent() {
    if (keys.length === 0) return null;
    for (let a = 0; a < keys.length; a++) {
      const idx = (currentIndex + a) % keys.length;
      if (Date.now() >= (quotaExhaustedUntil[keys[idx]] || 0)) { currentIndex = idx; return keys[idx]; }
    }
    return keys.reduce((a,b) => (quotaExhaustedUntil[a]||0)<(quotaExhaustedUntil[b]||0)?a:b);
  }
  function markQuotaExhausted(key) {
    quotaExhaustedUntil[key] = Date.now() + 60000;
    currentIndex = (currentIndex + 1) % keys.length;
  }
  function getStatus() {
    return { totalKeys: keys.length, currentKeyIndex: currentIndex,
      keyStatuses: keys.map((k,i) => ({ index:i, suffix:`...${k.slice(-6)}`,
        isExhausted: Date.now()<(quotaExhaustedUntil[k]||0),
        exhaustedUntil: quotaExhaustedUntil[k]?new Date(quotaExhaustedUntil[k]).toISOString():null })) };
  }
  if (keys.length===0) console.warn("[KeyManager] ⚠️  No Gemini API keys found!");
  else console.log(`[KeyManager] ✅ Loaded ${keys.length} API key(s).`);
  return { getCurrent, markQuotaExhausted, getStatus, get count(){return keys.length;} };
})();

app.use(cors());
app.use(express.json({ limit: "100mb" }));
app.use((req,res,next) => { res.setTimeout(600_000, ()=>res.status(503).json({error:"Timeout"})); next(); });

const distPath = join(__dirname, "dist");
app.use(express.static(distPath));

/* ══════════════════════════════════════════
   DYNAMIC MODEL DISCOVERY
══════════════════════════════════════════ */
const PREFERRED_ORDER = [
  "gemini-2.5-flash-preview-04-17","gemini-2.5-flash","gemini-2.5-pro",
  "gemini-2.0-flash","gemini-2.0-flash-lite","gemini-1.5-flash","gemini-1.5-pro",
];
let cachedModels = null, cacheTime = 0;
const CACHE_TTL = 3600000;

async function getAvailableModels(apiKey) {
  if (cachedModels && Date.now()-cacheTime < CACHE_TTL) return cachedModels;
  try {
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${apiKey||apiKeyManager.getCurrent()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const available = (data.models||[]).filter(m=>m.supportedGenerationMethods?.includes("generateContent")).map(m=>m.name.replace("models/",""));
    const sorted = [...PREFERRED_ORDER.filter(m=>available.includes(m)), ...available.filter(m=>!PREFERRED_ORDER.includes(m)&&m.startsWith("gemini"))];
    cachedModels = sorted.length>0 ? sorted : PREFERRED_ORDER;
    cacheTime = Date.now();
    return cachedModels;
  } catch(e) { return PREFERRED_ORDER; }
}

app.get("/api/health", (req,res) => res.json({ok:true,ts:Date.now(),keyManager:apiKeyManager.getStatus()}));
app.get("/api/key-status", (req,res) => res.json(apiKeyManager.getStatus()));
app.get("/api/models", async (req,res) => {
  const k = apiKeyManager.getCurrent();
  if (!k) return res.status(500).json({error:"No API key"});
  res.json({models: await getAvailableModels(k)});
});

/* ══════════════════════════════════════════════════════
   PDF PAGE → BASE64 PNG  (high-res, with optional crop)
══════════════════════════════════════════════════════ */
const tmpDir = "/tmp/jee-pdf-pages";
mkdirSync(tmpDir, { recursive: true });

async function pdfPageToBase64(pdfBase64, pageNumber, cropRegion, dpi=200) {
  if (!pageNumber || pageNumber < 1) return null;
  const sid = randomUUID();
  const pdfPath = join(tmpDir, `${sid}.pdf`);
  const outPrefix = join(tmpDir, sid);
  try {
    writeFileSync(pdfPath, Buffer.from(pdfBase64, "base64"));
    const pg = String(pageNumber);
    execSync(`pdftoppm -r ${dpi} -f ${pg} -l ${pg} -png "${pdfPath}" "${outPrefix}"`, {timeout:30000,stdio:"pipe"});
    const padded = pg.padStart(6,"0");
    let imgPath = `${outPrefix}-${padded}.png`;
    if (!existsSync(imgPath)) {
      for (let p=1;p<=5;p++) { const a=`${outPrefix}-${pg.padStart(p,"0")}.png`; if(existsSync(a)){imgPath=a;break;} }
    }
    if (!existsSync(imgPath)) return null;

    if (cropRegion && cropRegion.top!=null && cropRegion.bottom!=null) {
      const croppedPath = `${outPrefix}-c.png`;
      try {
        const dim = execSync(`identify -format "%wx%h" "${imgPath}"`,{stdio:"pipe"}).toString().trim();
        const [W,H] = dim.split("x").map(Number);
        // Add small padding so question doesn't get clipped
        const pad = 1.5;
        const t = Math.max(0, cropRegion.top - pad);
        const b = Math.min(100, cropRegion.bottom + pad);
        const l = Math.max(0, (cropRegion.left??0) - pad);
        const r = Math.min(100, (cropRegion.right??100) + pad);
        const x = Math.round(l/100*W), y = Math.round(t/100*H);
        const cw = Math.round((r-l)/100*W), ch = Math.round((b-t)/100*H);
        if (cw>10 && ch>10) {
          execSync(`convert "${imgPath}" -crop ${cw}x${ch}+${x}+${y} +repage "${croppedPath}"`,{timeout:15000,stdio:"pipe"});
          if (existsSync(croppedPath)) {
            const d = readFileSync(croppedPath).toString("base64");
            unlinkSync(croppedPath); unlinkSync(imgPath);
            return d;
          }
        }
      } catch(e) { console.error("[Crop] failed:",e.message); }
    }
    const d = readFileSync(imgPath).toString("base64");
    unlinkSync(imgPath);
    return d;
  } catch(e) { console.error(`[PDF2Img] page ${pageNumber}:`,e.message); return null; }
  finally { try{unlinkSync(pdfPath);}catch{} }
}

/* /api/page-image */
app.post("/api/page-image", async (req,res) => {
  const {base64, page, cropRegion, dpi} = req.body;
  if (!base64||!page) return res.status(400).json({error:"Missing base64 or page"});
  const image = await pdfPageToBase64(base64, page, cropRegion, dpi||200);
  if (!image) return res.status(500).json({error:"Could not render page. Is poppler-utils installed?"});
  return res.json({ok:true, image, page});
});

/* ══════════════════════════════════════════════════════
   CORE GEMINI CALL
══════════════════════════════════════════════════════ */
async function callGemini(apiKey, model, base64, promptText) {
  const key = apiKey || apiKeyManager.getCurrent();
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
    { method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify({ contents:[{parts:[
        {inline_data:{mime_type:"application/pdf",data:base64}},
        {text:promptText}
      ]}], generationConfig:{temperature:0.1,maxOutputTokens:65536} }) }
  );
  if (!res.ok) { const b=await res.text(); const e=new Error(`HTTP ${res.status}: ${b}`); e.status=res.status; throw e; }
  const data = await res.json();
  let txt = data.candidates?.[0]?.content?.parts?.[0]?.text||"";
  if (!txt) throw new Error(`Empty response. finishReason=${data.candidates?.[0]?.finishReason}`);
  txt = txt.replace(/^```json\s*/i,"").replace(/^```\s*/i,"").replace(/\s*```$/i,"").trim();
  const start=txt.indexOf("{"), end=txt.lastIndexOf("}");
  if (start===-1||end===-1) throw new Error("No JSON in response");
  return JSON.parse(txt.slice(start,end+1));
}

async function callGeminiWithFallback(apiKey, models, base64, promptText, validate) {
  let lastError = null;
  for (const model of models) {
    let rotations = 0;
    while (rotations <= apiKeyManager.count) {
      const key = apiKeyManager.getCurrent();
      try {
        const parsed = await callGemini(key, model, base64, promptText);
        if (validate && !validate(parsed)) { lastError=`${model}: validation failed`; break; }
        console.log(`[Gemini] ✅ ${model}`);
        return {parsed, model};
      } catch(err) {
        if (err.status===429) {
          apiKeyManager.markQuotaExhausted(key); rotations++;
          if (rotations<=apiKeyManager.count) continue;
          lastError=`${model}: all keys quota-exhausted`;
        } else {
          lastError=`${model}: ${err.message}`;
          if (err.status===404) cachedModels=null;
        }
        break;
      }
    }
  }
  throw new Error(lastError||"All models failed");
}

/* ══════════════════════════════════════════════════════════════════
   THE NEW APPROACH: Ask Gemini to locate EACH QUESTION as a crop region
   Returns: question body crop, options crop (or individual option crops),
   type, correct answer, marks, etc.
   The frontend then renders the actual PDF image crop for each question.
══════════════════════════════════════════════════════════════════ */
function buildQuestionLocatorPrompt(subject, startId) {
  return `You are analyzing a JEE exam PDF. Extract ONLY ${subject} questions. IDs start at ${startId}.

For EACH question return:
- id: sequential number starting at ${startId}
- subject: "${subject}"
- type: "mcq" or "integer"
- page: page number (integer)
- questionRegion: TIGHT bounding box of ONLY the question text + any diagram/figure. Do NOT include the A/B/C/D options. Values are % of page (0-100): {"top","bottom","left","right"}
- optionsRegion: For MCQ — bounding box covering ALL four options (A,B,C,D) together as one block. For integer — null.
- correct: 0-based index for MCQ (0=A,1=B,2=C,3=D), or the integer answer
- marks: 4
- negative: -1 for MCQ, 0 for integer

RULES:
- questionRegion must NOT overlap with optionsRegion
- Be TIGHT — typical questionRegion is 10-35% of page height
- Each question appears EXACTLY ONCE (skip answer key sections)
- Extract ALL ${subject} questions

Return ONLY valid JSON, no markdown:
{"questions":[
{"id":${startId},"subject":"${subject}","type":"mcq","page":2,"questionRegion":{"top":8,"bottom":32,"left":3,"right":97},"optionsRegion":{"top":33,"bottom":55,"left":3,"right":97},"correct":1,"marks":4,"negative":-1},
{"id":${startId+1},"subject":"${subject}","type":"integer","page":3,"questionRegion":{"top":60,"bottom":80,"left":3,"right":97},"optionsRegion":null,"correct":24,"marks":4,"negative":0}
]}`;
}

function buildAnswerKeyPrompt() {
  return `Extract the complete answer key from this JEE PDF.
Return ONLY valid JSON — no markdown:
{"answers":[{"q":1,"correct":1,"type":"mcq"},{"q":2,"correct":24,"type":"integer"},...]}
- q = question number (1-based)
- MCQ: correct = 0-based index (0=A, 1=B, 2=C, 3=D)
- integer: correct = the numeric answer
- Include ALL questions. Output ONLY JSON.`;
}

/* ══════════════════════════════════════════════════════════════════
   /api/parse-pdf  — main endpoint (new crop-based approach)
══════════════════════════════════════════════════════════════════ */
app.post("/api/parse-pdf", async (req, res) => {
  const geminiApiKey = apiKeyManager.getCurrent();
  if (!geminiApiKey) return res.status(500).json({error:"GEMINI_API_KEY not set."});
  const {base64, isKey, model: requestedModel} = req.body;
  if (!base64) return res.status(400).json({error:"Missing base64 PDF data"});

  const allModels = await getAvailableModels(geminiApiKey);
  const models = requestedModel && allModels.includes(requestedModel)
    ? [requestedModel, ...allModels.filter(m=>m!==requestedModel)]
    : allModels;

  if (isKey) {
    try {
      const {parsed,model} = await callGeminiWithFallback(null,models,base64,buildAnswerKeyPrompt(),
        p=>Array.isArray(p.answers)&&p.answers.length>0);
      return res.status(200).json({ok:true,data:parsed,modelUsed:model});
    } catch(err) { return res.status(502).json({error:err.message}); }
  }

  // Deduplication helper
  function dedupe(questions) {
    const seen = new Map();
    const out = [];
    for (const q of questions) {
      const key = `${q.page}:${Math.round((q.questionRegion?.top||0)/5)*5}:${q.subject}`;
      if (seen.has(key)) continue;
      seen.set(key, true);
      out.push(q);
    }
    return out;
  }

  const subjects = ["Physics","Chemistry","Mathematics"];
  const startIds = {Physics:1, Chemistry:31, Mathematics:61};

  const results = await Promise.allSettled(
    subjects.map(async (subject) => {
      const {parsed} = await callGeminiWithFallback(null, models, base64,
        buildQuestionLocatorPrompt(subject, startIds[subject]),
        p => Array.isArray(p.questions) && p.questions.length > 0);
      return parsed.questions.map(q => ({...q, subject}));
    })
  );

  const allQuestions = [];
  const failures = [];
  results.forEach((r,i) => {
    if (r.status==="fulfilled") allQuestions.push(...r.value);
    else failures.push(subjects[i]);
  });

  if (allQuestions.length > 0) {
    allQuestions.sort((a,b) => (a.id||0)-(b.id||0));
    const deduped = dedupe(allQuestions);
    const numbered = deduped.map((q,i) => ({...q, id:i+1}));
    return res.status(200).json({
      ok:true, data:{questions:numbered}, modelUsed:"parallel",
      warning: failures.length>0 ? `Could not extract ${failures.join(", ")}` : null
    });
  }

  // Fallback: single call for all subjects
  const fallbackPrompt = `Extract ALL questions from this JEE exam PDF (Physics, Chemistry, Mathematics).

For EACH question: id (sequential), subject, type ("mcq"/"integer"), page, questionRegion (tight crop of question text+diagram, NOT options), optionsRegion (one box covering all A/B/C/D options for MCQ, null for integer), correct (0-based index for MCQ, integer value for integer type), marks (4), negative (-1 MCQ, 0 integer).

Return ONLY valid JSON:
{"questions":[{"id":1,"subject":"Physics","type":"mcq","page":1,"questionRegion":{"top":5,"bottom":28,"left":3,"right":97},"optionsRegion":{"top":29,"bottom":48,"left":3,"right":97},"correct":2,"marks":4,"negative":-1},{"id":2,"subject":"Chemistry","type":"integer","page":2,"questionRegion":{"top":50,"bottom":72,"left":3,"right":97},"optionsRegion":null,"correct":5,"marks":4,"negative":0}]}

Extract ALL questions, no duplicates. Output ONLY JSON.`;

  try {
    const {parsed,model} = await callGeminiWithFallback(null,models,base64,fallbackPrompt,
      p=>Array.isArray(p.questions)&&p.questions.length>0);
    const deduped = dedupe(parsed.questions);
    const numbered = deduped.map((q,i) => ({...q,id:i+1}));
    return res.status(200).json({ok:true,data:{questions:numbered},modelUsed:model});
  } catch(err) { return res.status(502).json({error:err.message}); }
});

app.get("*", (req,res) => {
  const p = join(distPath,"index.html");
  existsSync(p) ? res.sendFile(p) : res.status(404).send("Not built yet.");
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ TestForge server on port ${PORT}`);
  try { execSync("which pdftoppm",{stdio:"pipe"}); console.log("✅ pdftoppm found"); }
  catch { console.warn("⚠️  Install poppler-utils: apt-get install -y poppler-utils"); }
});
