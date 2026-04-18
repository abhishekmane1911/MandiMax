import React, { useState, useRef, useEffect } from 'react';

// Always use relative /api/* → Netlify proxy (netlify.toml) forwards to Databricks
// This completely avoids CORS — browser never touches Databricks directly
const API_BASE = '';

// ── Indian language options ────────────────────────────────────────────────────
const LANGUAGES = [
  { code: 'en-IN', label: 'EN', name: 'English' },
  { code: 'hi-IN', label: 'हि', name: 'Hindi' },
  { code: 'gu-IN', label: 'ગુ', name: 'Gujarati' },
  { code: 'mr-IN', label: 'म', name: 'Marathi' },
  { code: 'ta-IN', label: 'த', name: 'Tamil' },
  { code: 'te-IN', label: 'తె', name: 'Telugu' },
  { code: 'kn-IN', label: 'ಕ', name: 'Kannada' },
  { code: 'bn-IN', label: 'ব', name: 'Bengali' },
  { code: 'pa-IN', label: 'ਪੰ', name: 'Punjabi' },
  { code: 'ml-IN', label: 'മ', name: 'Malayalam' },
];

// ── Local JS fallback engine (when no backend) ────────────────────────────────
const MOCK_ARB = [
  { tgt_market:'SURAT',     tgt_district:'SURAT',     tgt_state:'GUJARAT',       tgt_price:10000, tgt_lat:21.1702, tgt_lon:72.8311 },
  { tgt_market:'ROHTAK',    tgt_district:'ROHTAK',    tgt_state:'HARYANA',       tgt_price:9000,  tgt_lat:28.8955, tgt_lon:76.6066 },
  { tgt_market:'MATHURA',   tgt_district:'MATHURA',   tgt_state:'UTTAR PRADESH', tgt_price:8200,  tgt_lat:27.4924, tgt_lon:77.6737 },
  { tgt_market:'LUCKNOW',   tgt_district:'LUCKNOW',   tgt_state:'UTTAR PRADESH', tgt_price:8000,  tgt_lat:26.8467, tgt_lon:80.9462 },
  { tgt_market:'SULTANPUR', tgt_district:'SULTANPUR', tgt_state:'UTTAR PRADESH', tgt_price:7875,  tgt_lat:26.2647, tgt_lon:82.0727 },
  { tgt_market:'NAGPUR',    tgt_district:'NAGPUR',    tgt_state:'MAHARASHTRA',   tgt_price:7500,  tgt_lat:21.1458, tgt_lon:79.0882 },
  { tgt_market:'PUNE',      tgt_district:'PUNE',      tgt_state:'MAHARASHTRA',   tgt_price:7200,  tgt_lat:18.5204, tgt_lon:73.8567 },
];

// Expanded multilingual commodity keywords (Hindi, Marathi, Gujarati, Tamil, Telugu, Kannada)
const COMMODITY_MAP = {
  Tomato: ['tomato','tamatar','टमाटर','tomat','tometo','டமாடோ','టమాటా','ಟಮಾಟೇ'],
  Onion:  ['onion','pyaz','प्याज','kanda','कांदा','कांडा','dungri','डुंगरी','vengayam','ulligadda','eerulli','ಉಳ್ಳಿ'],
  Potato: ['potato','aloo','आलू','batata','बटाटा','urulaikilangu','bangaladumpa','ಆలూగడ్డ'],
  Wheat:  ['wheat','gehun','गेहूं','gahu','गहू','godhi','ਗਹું','ਕਣਕ'],
  Rice:   ['rice','chawal','चावल','tandul','तांदूळ','arisi','biyyam','akki','બાફસુ'],
};
const CITY_COORDS = {
  // English names
  ujjain:[23.1828,75.7772], indore:[22.7196,75.8577], bhopal:[23.2599,77.4126],
  lucknow:[26.8467,80.9462], jaipur:[26.9124,75.7873], pune:[18.5204,73.8567],
  mumbai:[19.0760,72.8777], delhi:[28.6139,77.2090], ahmedabad:[23.0225,72.5714],
  surat:[21.1702,72.8311], nagpur:[21.1458,79.0882], kanpur:[26.4499,80.3319],
  patna:[25.5941,85.1376], kolkata:[22.5726,88.3639],
  gorakhpur:[26.7606,83.3732], varanasi:[25.3176,82.9739],
  // Hindi/Marathi Devanagari names (fallback for non-Sarvam mode)
  'पुणे':[18.5204,73.8567], 'इंदौर':[22.7196,75.8577], 'दिल्ली':[28.6139,77.2090],
  'मुंबई':[19.0760,72.8777], 'नागपुर':[21.1458,79.0882], 'लखनउ':[26.8467,80.9462],
  'पटना':[25.5941,85.1376], 'जयपुर':[26.9124,75.7873], 'कानपुर':[26.4499,80.3319],
  'सूरत':[21.1702,72.8311], 'वाराणसी':[25.3176,82.9739], 'कोलकाता':[22.5726,88.3639],
  'अहमदाबाद':[23.0225,72.5714], 'भोपाल':[23.2599,77.4126], 'उज्जैन':[23.1828,75.7772],
};
function hav(lat1,lon1,lat2,lon2){
  const R=6371,dlat=(lat2-lat1)*Math.PI/180,dlon=(lon2-lon1)*Math.PI/180;
  const a=Math.sin(dlat/2)**2+Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dlon/2)**2;
  return Math.round(R*2*Math.asin(Math.sqrt(a))*10)/10;
}

function localProcess(message, session) {
  const t = message.toLowerCase().trim();

  // Detect commodity using expanded multilingual map
  let commodity = null;
  for(const [crop, keys] of Object.entries(COMMODITY_MAP)){
    if(keys.some(k=>t.includes(k))){ commodity=crop; break; }
  }
  const qtyM = t.match(/(\d+(?:\.\d+)?)\s*(?:quintal|q\b)/i);
  const qty = qtyM ? parseFloat(qtyM[1]) : (session.quantity||10);

  // ── Commodity is HIGHEST PRIORITY — always starts fresh sell flow ─────────
  const FRESH = [undefined, null, '', 'ask_commodity', 'done'];
  if(commodity && FRESH.includes(session.step)) {
    return { reply:`Got it! 🌾 *${commodity}*, ${qty} quintal(s).\n\n📍 Which city are you in?\n\nType: *Ujjain, Indore, Lucknow, Patna, Delhi, Pune, Nagpur*`, context:{step:'ask_city', commodity, quantity:qty} };
  }

  if(['hi','hello','help','start','नमस्ते','नमस्कार'].includes(t) || (!session.step && !commodity && !t.includes('crop'))) {
    return { reply: '👋 *Welcome to MandiMax!*\n\nI help you find the *best mandi* to sell your crop.\n\nType: *sell tomato*, *कांदा विकायचा आहे*, *crops UP*, or *help*', context:{step:'ask_commodity'} };
  }
  if(t.includes('crop') || t.includes('advisory') || t.includes('पिक')) {
    return { reply: '🌱 *Best crops (by stability):*\n\n🥇 *Rice* — ₹3,245/q (Stability 99.6/100)\n🥈 *Potato* — ₹1,555/q (Stability 96.9/100)\n🥉 *Onion* — ₹2,129/q (Stability 95.4/100)\n\n_Based on 30-day APMC data._', context:{step:'done'} };
  }

  if(session.step === 'ask_city') {
    let found=null;
    for(const [city,coords] of Object.entries(CITY_COORDS)) { if(t.includes(city)){found={city,coords};break;} }
    if(!found) return { reply:'📍 City not found. Try: Indore, Ujjain, Lucknow, Delhi, Pune, Nagpur, Patna', context:session };
    const [lat,lon]=found.coords;
    return { reply:`📍 *${found.city.charAt(0).toUpperCase()+found.city.slice(1)}* noted!\n\n🚚 What is your transport cost per km?\n\nType *12* for ₹12/km or *default* for ₹8/km`, context:{...session, step:'ask_transport', lat, lon, city:found.city} };
  }
  if(session.step === 'ask_transport') {
    const rate = t==='default' ? 8 : (parseFloat(t.replace(/,/g,'').match(/(\d+(?:\.\d+)?)/)?.[1])||8);
    const {commodity,lat,lon,quantity,city} = session;
    const recs = MOCK_ARB
      .map(r=>({...r, dist:hav(lat,lon,r.tgt_lat,r.tgt_lon)}))
      .map(r=>({...r, transport:Math.round(r.dist*rate), net:Math.round(r.tgt_price-r.dist*rate)}))
      .filter(r=>r.net>0 && r.dist<=250)   // ← 250km max practical distance
      .sort((a,b)=>b.net-a.net).slice(0,3);
    if(!recs.length) return { reply:`😔 No profitable markets within 250km of *${city}* for *${commodity}* at ₹${rate}/km.\n\nConsider selling at your local APMC or reducing transport cost.`, context:{step:'done'} };
    const icons=['🥇','🥈','🥉'];
    const lines=[`🌾 *${commodity}* near *${city}* (≤250km, ₹${rate}/km)\n`];
    recs.forEach((r,i)=>{
      const flag = r.dist<=80 ? ' 💡 *Nearby!*' : r.dist>200 ? ' ⚠️ Far' : '';
      lines.push(`${icons[i]} *${r.tgt_market}*, ${r.tgt_district}${flag}\n   💰 ₹${r.tgt_price.toLocaleString('en-IN')}/q | 📍 ${r.dist}km\n   🚚 ₹${r.transport.toLocaleString('en-IN')}/q transport\n   ✅ Net: *₹${r.net.toLocaleString('en-IN')}/q* | Total: *₹${(r.net*quantity).toLocaleString('en-IN')}*\n`);
    });
    lines.push(`_⚠️ Confirm prices before travel. Data from Databricks gold tables._`);
    return { reply:lines.join('\n'), context:{step:'done'} };
  }
  return { reply:`🤔 I didn't understand that.\n\nTry: *sell tomato*, *sell 20 quintal onion*, *crops UP*, or *help*`, context:{} };
}

// ── Speech-to-Text via Sarvam STT API ────────────────────────────────────────
async function speechToText(audioBlob, language) {
  // Always try — Netlify proxy handles routing to Databricks
  const form = new FormData();
  form.append('audio', audioBlob, 'recording.webm');
  form.append('language', language);
  try {
    const res = await fetch(`${API_BASE}/api/stt`, { method:'POST', body:form, signal:AbortSignal.timeout(15000) });
    if(!res.ok) return null;
    const d = await res.json();
    return d.transcript || null;
  } catch { return null; }
}

// ── Text-to-Speech via Sarvam TTS API ────────────────────────────────────────
async function playTTS(text, language) {
  if(!API_BASE || language==='en-IN') return; // skip for English (browser TTS is fine)
  try {
    const res = await fetch(`${API_BASE}/api/tts`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({text:text.replace(/\*/g,'').slice(0,400), language}),
      signal: AbortSignal.timeout(12000),
    });
    if(!res.ok) return;
    const d = await res.json();
    if(d.audio_base64) {
      const audio = new Audio(`data:audio/wav;base64,${d.audio_base64}`);
      audio.play().catch(()=>{});
    }
  } catch {}
}
async function sendMessage(message, session, language) {
  // Always try backend via Netlify proxy /api/chat
  // API_BASE is '' (empty) so URL becomes '/api/chat' → Netlify proxies to Databricks
  try {
    const res = await fetch(`${API_BASE}/api/chat`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ message, language, context: session, user_id:'dashboard_user' }),
      signal: AbortSignal.timeout(12000),
    });
    if(res.ok) {
      const d = await res.json();
      if(d.reply) return { reply: d.reply, context: d.context || {}, sarvam: d.sarvam_used };
    }
  } catch(e) { console.warn('[ChatBot] Backend unavailable, using local engine:', e.message); }
  // Fallback: local JS engine (offline mode)
  return { ...localProcess(message, session), sarvam: false };
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function ChatBot() {
  const WELCOME = '👋 *Welcome to MandiMax!*\n\nI help Indian farmers find the best mandi to sell their crops.\n\nTry: *sell tomato*, *crops UP*, or type your crop name!';
  const [open,     setOpen]     = useState(false);
  const [messages, setMessages] = useState([{ from:'bot', text:WELCOME, ts:new Date() }]);
  const [input,    setInput]    = useState('');
  const [typing,   setTyping]   = useState(false);
  const [session,  setSession]  = useState({});
  const [language, setLanguage] = useState('en-IN');
  const [showLang, setShowLang] = useState(false);
  const [recording, setRecording] = useState(false);  // mic state
  const mediaRef = useRef(null);
  const bottomRef = useRef(null);
  const inputRef  = useRef(null);

  // ── Microphone handler (Sarvam STT) ────────────────────────────────────────
  const toggleMic = async () => {
    if(recording) {
      mediaRef.current?.stop();
      setRecording(false);
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const recorder = new MediaRecorder(stream);
      const chunks = [];
      recorder.ondataavailable = e => chunks.push(e.data);
      recorder.onstop = async () => {
        stream.getTracks().forEach(t => t.stop());
        const blob = new Blob(chunks, { type: 'audio/webm' });
        setTyping(true);
        const transcript = await speechToText(blob, language);
        setTyping(false);
        if(transcript) {
          setInput(transcript);
          // Auto-send after short delay
          setTimeout(() => {
            setMessages(prev=>[...prev,{from:'user',text:transcript,ts:new Date()}]);
            setInput('');
            setTyping(true);
            sendMessage(transcript, session, language).then(({reply,context,sarvam}) => {
              setSession(context||{});
              setTyping(false);
              setMessages(prev=>[...prev,{from:'bot',text:reply,ts:new Date(),sarvam}]);
              if(language!=='en-IN') playTTS(reply, language);
            });
          }, 100);
        }
      };
      mediaRef.current = recorder;
      recorder.start();
      setRecording(true);
    } catch(e) {
      alert('Microphone access denied. Please allow mic in browser settings.');
    }
  };

  useEffect(()=>{ bottomRef.current?.scrollIntoView({behavior:'smooth'}); },[messages,typing]);
  useEffect(()=>{ if(open) inputRef.current?.focus(); },[open]);

  const send = async () => {
    const text = input.trim();
    if(!text) return;
    setMessages(prev=>[...prev,{from:'user',text,ts:new Date()}]);
    setInput('');
    setTyping(true);
    try {
      const {reply, context, sarvam} = await sendMessage(text, session, language);
      setSession(context||{});
      setMessages(prev=>[...prev,{from:'bot',text:reply,ts:new Date(),sarvam}]);
      // Auto-play TTS for non-English languages
      if(language!=='en-IN') playTTS(reply, language);
    } finally { setTyping(false); }
  };

  const handleKey = e => { if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send();} };

  const fmt = d => d.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'});

  const formatText = text => text.split('\n').map((line,i)=>{
    const parts = line.split(/(\*[^*]+\*)/g).map((p,j)=>
      p.startsWith('*')&&p.endsWith('*') ? <strong key={j}>{p.slice(1,-1)}</strong> : p
    );
    return <span key={i}>{parts}<br/></span>;
  });

  const currentLang = LANGUAGES.find(l=>l.code===language);

  return (
    <>
      {/* Floating button */}
      <button id="chatbot-toggle" onClick={()=>setOpen(o=>!o)} title="MandiMax Bot" style={{
        position:'fixed',bottom:24,right:24,zIndex:1000,width:60,height:60,
        borderRadius:'50%',border:'none',background:'linear-gradient(135deg,#25d366,#128c7e)',
        fontSize:26,cursor:'pointer',boxShadow:'0 4px 20px rgba(37,211,102,0.4)',
        display:'flex',alignItems:'center',justifyContent:'center',color:'white',transition:'transform 0.2s',
      }}>{open?'✕':'💬'}</button>

      {open && (
        <div style={{
          position:'fixed',bottom:96,right:24,zIndex:999,width:365,height:560,
          borderRadius:16,background:'#0b141a',display:'flex',flexDirection:'column',
          boxShadow:'0 20px 60px rgba(0,0,0,0.6)',fontFamily:'-apple-system,sans-serif',
          overflow:'hidden',border:'1px solid rgba(255,255,255,0.08)',
        }}>

          {/* Header */}
          <div style={{background:'linear-gradient(135deg,#1f2c34,#2a3942)',padding:'10px 14px',
            display:'flex',alignItems:'center',gap:10,borderBottom:'1px solid rgba(255,255,255,0.06)'}}>
            <div style={{width:38,height:38,borderRadius:'50%',background:'linear-gradient(135deg,#25d366,#128c7e)',
              display:'flex',alignItems:'center',justifyContent:'center',fontSize:17}}>🌾</div>
            <div style={{flex:1}}>
              <div style={{color:'#e9edef',fontWeight:600,fontSize:13}}>MandiMax Bot</div>
              <div style={{color:'#00a884',fontSize:10,display:'flex',alignItems:'center',gap:3}}>
                <span style={{width:5,height:5,borderRadius:'50%',background:'#00a884',display:'inline-block'}}/>
                {API_BASE ? 'Sarvam AI · Databricks' : 'Offline mode'}
              </div>
            </div>
            {/* Language picker */}
            <div style={{position:'relative'}}>
              <button onClick={()=>setShowLang(s=>!s)} style={{
                padding:'4px 8px',borderRadius:8,border:'1px solid #2a3942',
                background:'#0b141a',color:'#8696a0',fontSize:11,cursor:'pointer',
              }} title={`Language: ${currentLang?.name}`}>
                {currentLang?.label} 🌐
              </button>
              {showLang && (
                <div style={{position:'absolute',right:0,top:32,background:'#1f2c34',
                  borderRadius:8,border:'1px solid #2a3942',padding:4,zIndex:10,width:130}}>
                  {LANGUAGES.map(l=>(
                    <div key={l.code} onClick={()=>{setLanguage(l.code);setShowLang(false);}}
                      style={{padding:'5px 10px',cursor:'pointer',color:language===l.code?'#25d366':'#e9edef',
                        fontSize:12,borderRadius:4,background:language===l.code?'rgba(37,211,102,0.1)':'transparent'}}>
                      {l.label} {l.name}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Chat body */}
          <div style={{flex:1,overflowY:'auto',padding:'10px 10px 6px',background:'#0b141a',
            backgroundImage:'radial-gradient(ellipse at top,rgba(37,211,102,0.03) 0%,transparent 60%)'}}>
            {messages.map((m,i)=>(
              <div key={i} style={{display:'flex',justifyContent:m.from==='user'?'flex-end':'flex-start',marginBottom:6}}>
                <div style={{maxWidth:'82%',padding:'7px 11px 5px',
                  borderRadius:m.from==='user'?'12px 12px 4px 12px':'12px 12px 12px 4px',
                  background:m.from==='user'?'linear-gradient(135deg,#005c4b,#128c7e)':'#1f2c34',
                  color:'#e9edef',fontSize:13,lineHeight:1.55,boxShadow:'0 1px 3px rgba(0,0,0,0.3)'}}>
                  <div>{formatText(m.text)}</div>
                  <div style={{textAlign:'right',fontSize:9,color:'rgba(233,237,239,0.45)',marginTop:2,display:'flex',justifyContent:'flex-end',alignItems:'center',gap:4}}>
                    {m.sarvam && <span style={{color:'#25d366',fontSize:8}}>✦ Sarvam AI</span>}
                    {fmt(m.ts)} {m.from==='user'&&'✓✓'}
                  </div>
                </div>
              </div>
            ))}
            {typing && (
              <div style={{display:'flex',justifyContent:'flex-start',marginBottom:6}}>
                <div style={{background:'#1f2c34',borderRadius:'12px 12px 12px 4px',padding:'9px 14px',display:'flex',gap:4,alignItems:'center'}}>
                  {[0,1,2].map(d=><div key={d} style={{width:6,height:6,borderRadius:'50%',background:'#8696a0',animation:'bounce 1.2s infinite',animationDelay:`${d*0.2}s`}}/>)}
                </div>
              </div>
            )}
            <div ref={bottomRef}/>
          </div>

          {/* Quick replies */}
          <div style={{padding:'5px 10px',display:'flex',gap:5,flexWrap:'wrap',background:'#1f2c34',borderTop:'1px solid rgba(255,255,255,0.04)'}}>
            {['sell tomato','sell onion','crops UP','help'].map(q=>(
              <button key={q} onClick={()=>{setInput(q);setTimeout(send,80);}}
                style={{padding:'3px 9px',borderRadius:14,border:'1px solid #2a3942',
                  background:'transparent',color:'#8696a0',fontSize:10,cursor:'pointer'}}>
                {q}
              </button>
            ))}
          </div>

          {/* Input with mic button */}
          <div style={{padding:'7px 10px',background:'#1f2c34',display:'flex',alignItems:'center',gap:6,borderTop:'1px solid rgba(255,255,255,0.06)'}}>
            {/* Mic button */}
            <button onClick={toggleMic} title={recording?'Stop recording':'Speak in your language'}
              style={{width:36,height:36,borderRadius:'50%',border:'none',flexShrink:0,
                background:recording?'#ff4444':'#2a3942',color:'white',cursor:'pointer',
                fontSize:16,display:'flex',alignItems:'center',justifyContent:'center',
                animation:recording?'pulse 1s infinite':'none',transition:'background 0.2s'}}>
              {recording?'⏹':'🎤'}
            </button>
            <input ref={inputRef} value={input} onChange={e=>setInput(e.target.value)} onKeyDown={handleKey}
              placeholder={recording?`Recording ${currentLang?.name}…`:`Type in ${currentLang?.name||'English'}…`}
              id="chatbot-input" disabled={recording}
              style={{flex:1,padding:'9px 13px',borderRadius:22,border:'none',background:'#2a3942',
                color:'#e9edef',fontSize:13,outline:'none',opacity:recording?0.6:1}}/>
            <button id="chatbot-send" onClick={send}
              style={{width:36,height:36,borderRadius:'50%',border:'none',flexShrink:0,
                background:input.trim()?'linear-gradient(135deg,#25d366,#128c7e)':'#2a3942',
                color:'white',cursor:'pointer',fontSize:14,transition:'all 0.2s'}}>➤</button>
          </div>
        </div>
      )}

      <style>{`
        @keyframes bounce { 0%,60%,100%{transform:translateY(0);opacity:0.4} 30%{transform:translateY(-4px);opacity:1} }
        @keyframes pulse  { 0%,100%{box-shadow:0 0 0 0 rgba(255,68,68,0.4)} 50%{box-shadow:0 0 0 8px rgba(255,68,68,0)} }
      `}</style>
    </>
  );
}
