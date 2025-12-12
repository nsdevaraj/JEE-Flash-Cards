import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Download, Search, RefreshCw, ArrowLeft, ArrowRight, Menu, X, Database, Filter, Hash } from 'lucide-react';
import { fetchQuestions } from './services/dataService';
import { QuizQuestion, FetchStatus } from './types';
import Flashcard from './components/Flashcard';

const App: React.FC = () => {
  // State
  const [questions, setQuestions] = useState<QuizQuestion[]>([]);
  const [status, setStatus] = useState<FetchStatus>('idle');
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isFlipped, setIsFlipped] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedSubject, setSelectedSubject] = useState('All');
  const [isSidebarOpen, setSidebarOpen] = useState(false);
  
  // Navigation Input State
  const [jumpInput, setJumpInput] = useState('1');

  // Load Data
  useEffect(() => {
    const load = async () => {
      setStatus('loading');
      try {
        const data = await fetchQuestions();
        setQuestions(data);
        setStatus('success');
      } catch (err) {
        setStatus('error');
      }
    };
    load();
  }, []);

  // Extract Subjects
  const subjects = useMemo(() => {
    const uniqueSubjects = new Set(questions.map(q => q.subject).filter(Boolean));
    // Ensure common subjects are prioritized if they exist
    const common = ['phy', 'chem', 'math'];
    const sorted = Array.from(uniqueSubjects).sort();
    return ['All', ...sorted];
  }, [questions]);

  // Filter Logic
  const filteredQuestions = useMemo(() => {
    let result = questions;

    // 1. Filter by Subject
    if (selectedSubject !== 'All') {
      result = result.filter(q => q.subject === selectedSubject);
    }

    // 2. Filter by Search Query
    if (searchQuery.trim()) {
      const lowerQ = searchQuery.toLowerCase();
      result = result.filter(q => 
        q.question.toLowerCase().includes(lowerQ) || 
        q.solution?.toLowerCase().includes(lowerQ)
      );
    }

    return result;
  }, [questions, searchQuery, selectedSubject]);

  // Reset index when filters change
  useEffect(() => {
    setCurrentIndex(0);
    setIsFlipped(false);
  }, [selectedSubject, searchQuery]);

  // Sync jump input with current index
  useEffect(() => {
    setJumpInput((currentIndex + 1).toString());
  }, [currentIndex]);

  // Safe Current Question Extraction
  const activeQuestion = filteredQuestions[currentIndex];

  // Handlers
  const handleNext = useCallback(() => {
    if (currentIndex < filteredQuestions.length - 1) {
      setIsFlipped(false);
      setTimeout(() => setCurrentIndex(prev => prev + 1), 150);
    }
  }, [currentIndex, filteredQuestions.length]);

  const handlePrev = useCallback(() => {
    if (currentIndex > 0) {
      setIsFlipped(false);
      setTimeout(() => setCurrentIndex(prev => prev - 1), 150);
    }
  }, [currentIndex]);

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(e.target.value);
  };

  const handleJump = (e: React.FormEvent) => {
    e.preventDefault();
    const num = parseInt(jumpInput, 10);
    if (!isNaN(num) && num >= 1 && num <= filteredQuestions.length) {
      setIsFlipped(false);
      setCurrentIndex(num - 1);
      (document.activeElement as HTMLElement)?.blur(); // Remove focus after jump
    } else {
      // Revert to current valid index if input is invalid
      setJumpInput((currentIndex + 1).toString());
    }
  };

  const handleExport = () => {
    const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(filteredQuestions, null, 2));
    const downloadAnchorNode = document.createElement('a');
    downloadAnchorNode.setAttribute("href", dataStr);
    downloadAnchorNode.setAttribute("download", `jee_questions_${selectedSubject.toLowerCase()}_export.json`);
    document.body.appendChild(downloadAnchorNode);
    downloadAnchorNode.click();
    downloadAnchorNode.remove();
  };

  // Keyboard Navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Ignore navigation shortcuts if user is typing in an input
      if (document.activeElement?.tagName === 'INPUT') return;

      if (e.key === 'ArrowRight') handleNext();
      if (e.key === 'ArrowLeft') handlePrev();
      if (e.key === ' ' || e.key === 'Enter') {
         setIsFlipped(prev => !prev);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleNext, handlePrev]);

  // UI Components
  if (status === 'loading') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-900 text-white">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
        <span className="ml-4 text-lg">Loading Question Bank...</span>
      </div>
    );
  }

  if (status === 'error') {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-slate-900 text-white p-6 text-center">
         <Database className="w-16 h-16 text-red-500 mb-4" />
        <h1 className="text-2xl font-bold mb-2">Error Loading Data</h1>
        <p className="text-slate-400 mb-6">Could not fetch questions. Check your connection or the data source.</p>
        <button 
          onClick={() => window.location.reload()}
          className="px-6 py-2 bg-blue-600 rounded-lg hover:bg-blue-500 transition"
        >
          Retry
        </button>
      </div>
    );
  }

  const progress = filteredQuestions.length > 0 
    ? ((currentIndex + 1) / filteredQuestions.length) * 100 
    : 0;

  return (
    <div className="min-h-screen bg-slate-900 text-slate-100 flex flex-col overflow-hidden relative">
      
      {/* Header */}
      <header className="flex flex-col gap-4 px-6 py-4 md:px-12 md:py-6 z-20 border-b border-white/5 bg-slate-900/50 backdrop-blur-sm">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl md:text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
              JEE Flashcards
            </h1>
            <p className="text-xs md:text-sm text-slate-500 mt-1">
              {filteredQuestions.length} questions • {selectedSubject}
            </p>
          </div>

          <div className="flex items-center gap-4">
             {/* Desktop Search */}
             <div className="hidden md:flex items-center bg-slate-800/50 border border-white/10 rounded-full px-4 py-2 focus-within:border-blue-500/50 transition-colors">
                <Search className="w-4 h-4 text-slate-400" />
                <input 
                  type="text" 
                  placeholder="Search..." 
                  className="bg-transparent border-none focus:ring-0 text-sm ml-2 w-32 text-white placeholder-slate-500 outline-none"
                  value={searchQuery}
                  onChange={handleSearch}
                />
             </div>

             <button 
               onClick={handleExport}
               className="p-2 rounded-full hover:bg-white/10 text-slate-400 hover:text-white transition"
               title="Export Filtered Questions"
             >
               <Download className="w-5 h-5" />
             </button>
             
             <button 
               onClick={() => setSidebarOpen(!isSidebarOpen)}
               className="md:hidden p-2 text-slate-300"
             >
               <Menu className="w-6 h-6" />
             </button>
          </div>
        </div>

        {/* Subject Filter Tabs */}
        {subjects.length > 1 && (
          <div className="flex items-center gap-2 overflow-x-auto pb-2 no-scrollbar mask-gradient">
             <Filter className="w-4 h-4 text-slate-500 flex-shrink-0 mr-2" />
             {subjects.map(sub => (
               <button
                 key={sub}
                 onClick={() => setSelectedSubject(sub)}
                 className={`
                   px-4 py-1.5 rounded-full text-xs md:text-sm font-medium whitespace-nowrap transition-all duration-200
                   ${selectedSubject === sub 
                     ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/25' 
                     : 'bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-white border border-white/5'}
                 `}
               >
                 {sub}
               </button>
             ))}
          </div>
        )}
      </header>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col items-center justify-center relative px-4 z-10 pt-4">
        
        {filteredQuestions.length === 0 ? (
          <div className="text-center p-12 bg-slate-800/30 rounded-3xl border border-white/5 backdrop-blur-sm">
             <Search className="w-12 h-12 text-slate-600 mx-auto mb-4" />
             <h3 className="text-xl font-semibold text-white">No results found</h3>
             <p className="text-slate-500 mt-2">
               No questions found for <span className="text-blue-400">"{searchQuery}"</span> in {selectedSubject}.
             </p>
             <div className="flex gap-4 justify-center mt-6">
                <button 
                  onClick={() => setSearchQuery('')}
                  className="px-4 py-2 bg-slate-700 rounded-lg text-sm hover:bg-slate-600 transition"
                >
                  Clear Search
                </button>
                <button 
                  onClick={() => setSelectedSubject('All')}
                  className="px-4 py-2 bg-blue-600 rounded-lg text-sm hover:bg-blue-500 transition"
                >
                  Reset Filters
                </button>
             </div>
          </div>
        ) : activeQuestion ? (
          <>
            <div className="flex items-center justify-center w-full gap-4 md:gap-12">
               {/* Prev Button (Desktop) */}
               <button 
                 onClick={handlePrev}
                 disabled={currentIndex === 0}
                 className={`hidden md:flex p-4 rounded-full border border-white/10 bg-slate-800/50 backdrop-blur-md transition-all duration-300 ${currentIndex === 0 ? 'opacity-30 cursor-not-allowed' : 'hover:bg-blue-600 hover:border-blue-500 hover:scale-110'}`}
               >
                 <ArrowLeft className="w-6 h-6" />
               </button>

               <Flashcard 
                  key={activeQuestion.id}
                  data={activeQuestion} 
                  isFlipped={isFlipped} 
                  onFlip={() => setIsFlipped(!isFlipped)} 
               />

               {/* Next Button (Desktop) */}
               <button 
                 onClick={handleNext}
                 disabled={currentIndex === filteredQuestions.length - 1}
                 className={`hidden md:flex p-4 rounded-full border border-white/10 bg-slate-800/50 backdrop-blur-md transition-all duration-300 ${currentIndex === filteredQuestions.length - 1 ? 'opacity-30 cursor-not-allowed' : 'hover:bg-emerald-600 hover:border-emerald-500 hover:scale-110'}`}
               >
                 <ArrowRight className="w-6 h-6" />
               </button>
            </div>
          </>
        ) : (
             <div className="animate-pulse flex flex-col items-center">
                <div className="h-64 w-96 bg-slate-800 rounded-3xl mb-4"></div>
                <div className="h-4 w-32 bg-slate-800 rounded"></div>
             </div>
        )}
      </main>

      {/* Footer Controls & Progress */}
      <footer className="w-full px-6 py-8 md:px-12 flex flex-col items-center gap-6 z-20">
         {filteredQuestions.length > 0 && (
           <div className="w-full max-w-2xl flex items-center gap-4">
             <button onClick={handlePrev} disabled={currentIndex === 0} className="md:hidden p-2 text-slate-400 disabled:opacity-30">
                <ArrowLeft className="w-5 h-5" />
             </button>
             
             <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden">
               <div 
                 className="h-full bg-gradient-to-r from-blue-500 to-emerald-500 transition-all duration-500 ease-out"
                 style={{ width: `${progress}%` }}
               />
             </div>
             
             {/* Go to Question Input */}
             <form onSubmit={handleJump} className="flex items-center min-w-[100px] justify-end">
                <input
                  type="text"
                  value={jumpInput}
                  onChange={(e) => setJumpInput(e.target.value)}
                  className="w-10 bg-transparent text-right border-b border-slate-700 focus:border-blue-500 outline-none text-slate-300 font-mono text-sm transition-colors focus:text-white"
                  aria-label="Go to question number"
                />
                <span className="text-sm font-mono text-slate-500 ml-1">/ {filteredQuestions.length}</span>
             </form>

             <button onClick={handleNext} disabled={currentIndex === filteredQuestions.length - 1} className="md:hidden p-2 text-slate-400 disabled:opacity-30">
                <ArrowRight className="w-5 h-5" />
             </button>
           </div>
         )}
         
         <div className="flex gap-6 text-slate-600 text-xs uppercase tracking-widest font-semibold">
            <span className="flex items-center gap-2"><div className="w-2 h-2 rounded-full bg-blue-500"></div> Space to Flip</span>
            <span className="flex items-center gap-2 hidden md:flex"><div className="w-2 h-2 rounded-full bg-slate-500"></div> Arrows to Navigate</span>
         </div>
      </footer>

      {/* Mobile Sidebar */}
      {isSidebarOpen && (
        <div className="fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm md:hidden">
          <div className="absolute right-0 top-0 bottom-0 w-3/4 bg-slate-900 border-l border-white/10 p-6 shadow-2xl overflow-y-auto">
             <div className="flex justify-between items-center mb-8">
               <h2 className="text-lg font-bold">Menu</h2>
               <button onClick={() => setSidebarOpen(false)}><X className="w-6 h-6 text-slate-400" /></button>
             </div>
             
             <div className="space-y-6">
               <div>
                 <label className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2 block">Search</label>
                 <div className="flex items-center bg-slate-800 rounded-lg px-3 py-3">
                   <Search className="w-5 h-5 text-slate-400 mr-2" />
                   <input 
                      type="text" 
                      placeholder="Search..." 
                      className="bg-transparent w-full text-white focus:outline-none"
                      value={searchQuery}
                      onChange={handleSearch}
                   />
                 </div>
               </div>

               <div>
                 <label className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2 block">Go to Question</label>
                 <form onSubmit={(e) => { handleJump(e); setSidebarOpen(false); }} className="flex items-center bg-slate-800 rounded-lg px-3 py-3">
                   <Hash className="w-5 h-5 text-slate-400 mr-2" />
                   <input 
                      type="number" 
                      placeholder={`1 - ${filteredQuestions.length}`}
                      className="bg-transparent w-full text-white focus:outline-none"
                      value={jumpInput}
                      onChange={(e) => setJumpInput(e.target.value)}
                   />
                   <button type="submit" className="text-xs bg-blue-600 px-2 py-1 rounded text-white ml-2">Go</button>
                 </form>
               </div>

                <div>
                 <label className="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2 block">Subject</label>
                 <div className="flex flex-wrap gap-2">
                    {subjects.map(sub => (
                       <button
                         key={sub}
                         onClick={() => { setSelectedSubject(sub); setSidebarOpen(false); }}
                         className={`px-3 py-2 rounded-lg text-sm ${selectedSubject === sub ? 'bg-blue-600 text-white' : 'bg-slate-800 text-slate-400'}`}
                       >
                         {sub}
                       </button>
                    ))}
                 </div>
               </div>

               <button 
                 onClick={handleExport}
                 className="w-full flex items-center justify-center gap-2 bg-slate-800 py-3 rounded-lg text-slate-200 hover:bg-slate-700 font-medium"
               >
                 <Download className="w-5 h-5" /> Export JSON
               </button>
               
               <button 
                 onClick={() => window.location.reload()}
                 className="w-full flex items-center justify-center gap-2 border border-slate-700 py-3 rounded-lg text-slate-400 hover:text-white"
               >
                 <RefreshCw className="w-4 h-4" /> Reload Data
               </button>
             </div>
          </div>
        </div>
      )}

      {/* Background Decor */}
      <div className="absolute top-0 left-0 w-full h-full overflow-hidden -z-10 pointer-events-none">
          <div className="absolute top-[-10%] left-[-10%] w-[50%] h-[50%] bg-blue-600/10 rounded-full blur-[120px]" />
          <div className="absolute bottom-[-10%] right-[-10%] w-[50%] h-[50%] bg-emerald-600/10 rounded-full blur-[120px]" />
      </div>
    </div>
  );
};

export default App;