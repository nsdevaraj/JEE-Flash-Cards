import React from 'react';
import { motion } from 'framer-motion';
import { QuizQuestion } from '../types';
import LatexRenderer from './LatexRenderer';

interface FlashcardProps {
  data: QuizQuestion;
  isFlipped: boolean;
  onFlip: () => void;
}

const Flashcard: React.FC<FlashcardProps> = ({ data, isFlipped, onFlip }) => {
  // If options array is empty, it means options are likely embedded in the question text.
  // In this case, we align text to the left for better readability of multi-line questions.
  const hasSeparateOptions = data.options && data.options.length > 0;

  return (
    <div className="relative w-full max-w-2xl h-[600px] perspective-1000 group cursor-pointer" onClick={onFlip}>
      <motion.div
        className="w-full h-full relative preserve-3d"
        initial={false}
        animate={{ rotateY: isFlipped ? 180 : 0 }}
        transition={{ duration: 0.6, type: "spring", stiffness: 260, damping: 20 }}
        style={{ transformStyle: 'preserve-3d' }}
      >
        {/* FRONT */}
        <div 
            className="absolute inset-0 backface-hidden w-full h-full bg-slate-800/90 backdrop-blur-xl border border-white/10 rounded-3xl flex flex-col shadow-2xl overflow-hidden"
            style={{ backfaceVisibility: 'hidden' }}
        >
          {/* Header (Fixed) */}
          <div className="w-full p-6 md:p-8 pb-2 flex items-center gap-2 shrink-0 z-10">
             <span className="text-xs font-bold tracking-widest text-slate-400 uppercase bg-slate-900/50 px-2 py-1 rounded">Question</span>
             {data.subject && <span className="text-xs font-bold tracking-widest text-blue-400 uppercase bg-blue-900/20 px-2 py-1 rounded">{data.subject}</span>}
          </div>
          
          {/* Scrollable Content */}
          <div className="flex-1 w-full overflow-y-auto custom-scrollbar relative">
            <div className="min-h-full flex flex-col justify-center p-6 md:p-8 pt-2">
                {/* Removed whitespace-pre-wrap to let Markdown/HTML control the layout (tables, etc.) */}
                <div className={`w-full font-medium leading-relaxed text-slate-200 ${hasSeparateOptions ? 'text-center text-xl md:text-2xl' : 'text-left text-base md:text-lg'}`}>
                  <LatexRenderer text={data.question} block={true} />
                </div>
                
                {hasSeparateOptions && (
                  <div className="w-full space-y-3 mt-8">
                    {data.options.map((opt, idx) => (
                      <div key={idx} className="flex items-start p-3 rounded-lg bg-white/5 border border-white/5 hover:bg-white/10 transition-colors">
                        <span className="font-mono text-xs text-blue-400 mr-3 mt-1 shrink-0">
                          {String.fromCharCode(65 + idx)}.
                        </span>
                        <div className="text-sm md:text-base text-slate-200">
                          <LatexRenderer text={opt.text} />
                        </div>
                      </div>
                    ))}
                  </div>
                )}
            </div>
          </div>

          {/* Footer (Fixed) */}
          <div className="w-full p-4 text-center shrink-0">
            <div className="text-xs md:text-sm text-slate-500 font-medium animate-pulse">
              Tap to reveal answer
            </div>
          </div>
        </div>

        {/* BACK */}
        <div 
            className="absolute inset-0 backface-hidden w-full h-full bg-slate-900/95 backdrop-blur-xl border border-emerald-500/30 rounded-3xl flex flex-col shadow-2xl shadow-emerald-900/20 overflow-hidden"
            style={{ backfaceVisibility: 'hidden', transform: 'rotateY(180deg)' }}
        >
          {/* Header */}
          <div className="w-full p-6 md:p-8 pb-2 shrink-0 z-10">
            <span className="text-xs font-bold tracking-widest text-emerald-500 uppercase bg-emerald-900/20 px-2 py-1 rounded">
                Answer
            </span>
          </div>
          
          {/* Scrollable Content */}
          <div className="flex-1 w-full overflow-y-auto custom-scrollbar relative">
             <div className="min-h-full flex flex-col justify-center items-center p-6 md:p-8 pt-2 text-center">
                <div className="text-4xl md:text-5xl font-bold text-white mb-8 tracking-tight">
                   <LatexRenderer text={data.answer} />
                </div>
                
                {data.solution && (
                  <div className="bg-emerald-950/30 border border-emerald-500/20 p-6 rounded-xl text-left w-full max-w-lg">
                    <p className="text-xs text-emerald-400 mb-3 font-bold uppercase tracking-wider flex items-center gap-2">
                      <span className="w-1 h-4 bg-emerald-500 rounded-full"></span> Explanation
                    </p>
                    {/* Removed whitespace-pre-wrap here too for consistent markdown rendering */}
                    <div className="text-slate-300 text-sm md:text-base leading-relaxed">
                       <LatexRenderer text={data.solution} block={true} />
                    </div>
                  </div>
                )}
             </div>
          </div>
          
           {/* Footer */}
           <div className="w-full p-4 text-center shrink-0">
             <div className="text-xs md:text-sm text-slate-500 font-medium">
              Tap to flip back
            </div>
          </div>
        </div>
      </motion.div>
      
      {/* Glow Effect behind */}
      <div className={`absolute -inset-4 bg-gradient-to-r ${isFlipped ? 'from-emerald-600/30 to-teal-600/30' : 'from-blue-600/30 to-indigo-600/30'} opacity-40 blur-3xl -z-10 transition-colors duration-500`} />
    </div>
  );
};

export default Flashcard;