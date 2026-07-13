//+------------------------------------------------------------------+
//| FXPMTermsExporter.mq5                                            |
//| Non-trading service: exports broker swap terms through FILE_COMMON|
//+------------------------------------------------------------------+
#property strict
#property version   "1.00"
#property description "Nonce-bound read-only FXPM broker terms exporter"

input string RequestFileName  = "fxpm_terms_request.csv";
input string ResponseFileName = "fxpm_terms_response.csv";
input int    PollSeconds       = 2;

#define REQUEST_SCHEMA "fxpm-mql5-broker-terms-request-v1"
#define RESPONSE_SCHEMA "fxpm-mql5-broker-terms-export-v1"

struct RequestRow
  {
   string nonce;
   string requested_at;
   string canonical;
   string broker_symbol;
   string expected_server;
   string expected_company;
   string expected_currency;
  };

string last_processed_nonce="";

string UtcIso(datetime value)
  {
   MqlDateTime parts;
   TimeToStruct(value,parts);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
                       parts.year,parts.mon,parts.day,
                       parts.hour,parts.min,parts.sec);
  }

bool ReadRequest(RequestRow &rows[],string &nonce)
  {
   int handle=FileOpen(RequestFileName,FILE_COMMON|FILE_READ|FILE_CSV|FILE_ANSI,',');
   if(handle==INVALID_HANDLE)
     return false;

   string expected_header[8]={"schema_version","nonce","requested_at_utc",
                              "canonical_symbol","broker_symbol","expected_server",
                              "expected_company","expected_currency"};
   for(int column=0;column<8;column++)
     {
      string observed=FileReadString(handle);
      if(observed!=expected_header[column])
        {
         FileClose(handle);
         Print("FXPM terms request header mismatch at column ",column);
         return false;
        }
     }

   ArrayResize(rows,0);
   nonce="";
   string server=AccountInfoString(ACCOUNT_SERVER);
   string company=TerminalInfoString(TERMINAL_COMPANY);
   string currency=AccountInfoString(ACCOUNT_CURRENCY);
   while(!FileIsEnding(handle))
     {
      string schema=FileReadString(handle);
      if(schema=="" && FileIsEnding(handle))
         break;
      RequestRow row;
      row.nonce=FileReadString(handle);
      row.requested_at=FileReadString(handle);
      row.canonical=FileReadString(handle);
      row.broker_symbol=FileReadString(handle);
      row.expected_server=FileReadString(handle);
      row.expected_company=FileReadString(handle);
      row.expected_currency=FileReadString(handle);
      if(schema!=REQUEST_SCHEMA || StringLen(row.nonce)<16 ||
         row.canonical=="" || row.broker_symbol=="")
        {
         FileClose(handle);
         Print("FXPM terms request contains an invalid row");
         return false;
        }
      if((row.expected_server!="" && row.expected_server!=server) ||
         (row.expected_company!="" && row.expected_company!=company) ||
         (row.expected_currency!="" && row.expected_currency!=currency))
        {
         FileClose(handle);
         Print("FXPM terms request broker identity mismatch for ",row.canonical);
         return false;
        }
      if(nonce=="") nonce=row.nonce;
      if(row.nonce!=nonce)
        {
         FileClose(handle);
         Print("FXPM terms request contains multiple nonces");
         return false;
        }
      int size=ArraySize(rows);
      if(size>=1000)
        {
         FileClose(handle);
         Print("FXPM terms request exceeds 1000 rows");
         return false;
        }
      ArrayResize(rows,size+1);
      rows[size]=row;
     }
   FileClose(handle);
   return ArraySize(rows)>0;
  }

string D1Anchors(string symbol)
  {
   string result="";
   // Sixty-four recent D1 anchors let a fresh terms snapshot bind to an
   // already-active price activation without exporting/replacing its bars.
   for(int shift=1;shift<=64;shift++)
     {
      datetime opened=iTime(symbol,PERIOD_D1,shift);
      if(opened<=0) continue;
      MqlDateTime parts;
      TimeToStruct(opened,parts);
      string token=StringFormat("%I64d:%d",(long)opened,parts.day_of_week);
      if(result!="") result+="|";
      result+=token;
     }
   return result;
  }

bool WriteResponse(RequestRow &rows[],string nonce)
  {
   string temporary=ResponseFileName+"."+nonce+".tmp";
   int handle=FileOpen(temporary,FILE_COMMON|FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(handle==INVALID_HANDLE)
     {
      Print("FXPM cannot open temporary terms response: ",GetLastError());
      return false;
     }
   FileWrite(handle,
             "schema_version","nonce","captured_at_utc","canonical_symbol",
             "broker_symbol","broker_server","terminal_company","account_currency",
             "terminal_build","swap_mode","swap_long","swap_short",
             "swap_rollover3days","swap_sunday","swap_monday","swap_tuesday",
             "swap_wednesday","swap_thursday","swap_friday","swap_saturday",
             "d1_anchors");

   string server=AccountInfoString(ACCOUNT_SERVER);
   string company=TerminalInfoString(TERMINAL_COMPANY);
   string currency=AccountInfoString(ACCOUNT_CURRENCY);
   long build=TerminalInfoInteger(TERMINAL_BUILD);
   string captured=UtcIso(TimeGMT());
   for(int index=0;index<ArraySize(rows);index++)
     {
      string symbol=rows[index].broker_symbol;
      if(!SymbolSelect(symbol,true))
        {
         FileClose(handle);
         FileDelete(temporary,FILE_COMMON);
         Print("FXPM cannot select broker symbol ",symbol);
         return false;
        }
      long swap_mode=0;
      long rollover_day=0;
      if(!SymbolInfoInteger(symbol,SYMBOL_SWAP_MODE,swap_mode) ||
         !SymbolInfoInteger(symbol,SYMBOL_SWAP_ROLLOVER3DAYS,rollover_day))
        {
         FileClose(handle);
         FileDelete(temporary,FILE_COMMON);
         Print("FXPM cannot read integer swap properties for ",symbol);
         return false;
        }
      double swap_long=0,swap_short=0;
      double factors[7];
      bool ok=SymbolInfoDouble(symbol,SYMBOL_SWAP_LONG,swap_long) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_SHORT,swap_short) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_SUNDAY,factors[0]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_MONDAY,factors[1]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_TUESDAY,factors[2]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_WEDNESDAY,factors[3]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_THURSDAY,factors[4]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_FRIDAY,factors[5]) &&
              SymbolInfoDouble(symbol,SYMBOL_SWAP_SATURDAY,factors[6]);
      string anchors=D1Anchors(symbol);
      if(!ok || anchors=="")
        {
         FileClose(handle);
         FileDelete(temporary,FILE_COMMON);
         Print("FXPM cannot read complete direct terms/anchors for ",symbol);
         return false;
        }
      FileWrite(handle,RESPONSE_SCHEMA,nonce,captured,rows[index].canonical,
                symbol,server,company,currency,build,swap_mode,swap_long,swap_short,
                rollover_day,factors[0],factors[1],factors[2],factors[3],
                factors[4],factors[5],factors[6],anchors);
     }
   FileFlush(handle);
   FileClose(handle);
   FileDelete(ResponseFileName,FILE_COMMON);
   if(!FileMove(temporary,FILE_COMMON,ResponseFileName,FILE_COMMON|FILE_REWRITE))
     {
      Print("FXPM cannot publish terms response atomically: ",GetLastError());
      FileDelete(temporary,FILE_COMMON);
      return false;
     }
   return true;
  }

void PollRequest()
  {
   RequestRow rows[];
   string nonce;
   if(!ReadRequest(rows,nonce)) return;
   if(nonce==last_processed_nonce) return;
   if(WriteResponse(rows,nonce))
     {
      last_processed_nonce=nonce;
      Print("FXPM broker terms exported for nonce ",nonce,
            " and ",ArraySize(rows)," symbols");
     }
  }

int OnInit()
  {
   EventSetTimer(MathMax(1,PollSeconds));
   PollRequest();
   return INIT_SUCCEEDED;
  }

void OnTimer()
  {
   PollRequest();
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
  }
