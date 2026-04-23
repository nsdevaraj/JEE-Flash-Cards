import { QuizQuestion, QuizOption } from '../types';
import { parquetRead } from 'hyparquet';
import { Buffer } from 'buffer';

window.Buffer = window.Buffer || Buffer;


const DATA_URL = `${import.meta.env.BASE_URL}mathnet.json`;
const STORAGE_KEY = 'jee_flashcards_cache_v3'; // Bumped version for new data structure

// Fallback data in case local fetch fails (e.g. if file not served correctly in some envs)
const FALLBACK_DATA: QuizQuestion[] = [
  {
    id: 1,
    subject: "Mathematics",
    question: "If $f(x) = x^2 + 2x + 1$, what is $f(1)$?",
    options: [],
    answer: "4",
    solution: "Substitute $x=1$: $f(1) = 1^2 + 2(1) + 1 = 1 + 2 + 1 = 4$."
  }
];

const mapSubject = (shortSubject: string): string => {
  const map: Record<string, string> = {
    'algebra': 'Algebra',
    'geometry': 'Geometry',
    'number theory': 'Number Theory',
    'discrete mathematics': 'Discrete Mathematics',
    'combinatorics': 'Counting & Probability',
    'counting & probability': 'Counting & Probability',
    'prealgebra': 'Prealgebra',
    'precalculus': 'Precalculus',
    'intermediate algebra': 'Intermediate Algebra'
  };
  const lower = (shortSubject || '').toLowerCase();
  for (const [key, value] of Object.entries(map)) {
      if (lower.includes(key)) return value;
  }
  return 'Algebra'; // default fallback
};

/**
 * Normalizes JEE Question Bank JSON data into our strict QuizQuestion format.
 */
const normalizeData = (data: any): QuizQuestion[] => {
  // The JEE json has a root "questions" array, MathNet format is direct array
  const list = Array.isArray(data.questions) ? data.questions : (Array.isArray(data) ? data : []);

  return list.map((item: any, index: number): QuizQuestion | null => {
    if (!item || typeof item !== 'object') return null;

    // Use 'gold' as answer, 'index' as ID
    const qText = item.question || "Question Text Missing";
    const answer = item.gold || item.answer || "See Solution";
    const subject = mapSubject(item.subject);
    
    // In this dataset, options are often embedded in the question text or not provided separately.
    // We will keep options empty and let the Question renderer handle the full text.
    const options: QuizOption[] = item.options || [];

    // Ensure ID is a number
    const idVal = Number(item.index || item.id);
    const finalId = !isNaN(idVal) && idVal !== 0 ? idVal : index + 1;

    return {
        id: finalId,
        question: qText,
        options,
        answer: String(answer),
        solution: item.solution || "", // Solution might not be present in this specific JSON
        subject: String(subject)
    };
  }).filter((q): q is QuizQuestion => q !== null && !!q.question); 
};

export const fetchQuestions = async (): Promise<QuizQuestion[]> => {
  try {
    const DATA_URL_PARQUET = `${import.meta.env.BASE_URL}dataset.parquet`;
    const response = await fetch(DATA_URL_PARQUET);
    if (!response.ok) {
        throw new Error(`Network response was not ok: ${response.status}`);
    }
    
    const arrayBuffer = await response.arrayBuffer();
    
    return new Promise((resolve, reject) => {
        const parsed: any[] = [];
        parquetRead({
            file: arrayBuffer,
            rowFormat: 'object',
            onComplete: (data) => {
                for (let j = 0; j < data.length; j++) {
                    let row = data[j];
                    const rawTopics = row.topics || row.topics_flat || [];
                    const topics = Array.isArray(rawTopics) ? rawTopics : [rawTopics];
                    let problem = Array.isArray(row.problem_markdown) ? row.problem_markdown.join('\n') : String(row.problem_markdown || row.original_problem_markdown || '');
                    let answerList = row.final_answer || [];
                    let answer = Array.isArray(answerList) ? answerList.join('\n') : String(answerList);
                    if (!answer || answer.length < 2 || answer === "undefined" || answer === "null") answer = "Detailed Solution Provided";
                    const solutionList = row.solutions_markdown || [];
                    let solution = Array.isArray(solutionList) ? solutionList.join('\n') : String(solutionList);

                    if (!problem || problem.length < 10) continue;

                    let subject = "Other";
                    const ts = JSON.stringify(topics).toLowerCase();
                    subject = mapSubject(ts);

                    parsed.push({
                        id: parsed.length + 1,
                        subject: subject,
                        question: problem,
                        options: [],
                        answer: answer,
                        solution: solution
                    });
                }
                const normalizedData = normalizeData(parsed);
                if (normalizedData.length === 0) {
                    reject(new Error("Parsed data resulted in 0 valid questions"));
                } else {
                    console.log(`Loaded ${normalizedData.length} questions from parquet.`);
                    resolve(normalizedData);
                }
            }
        }).catch(reject);
    });

  } catch (error) {
    console.warn("Fetch failed, attempting to load from cache or fallback...", error);
    return FALLBACK_DATA;
  }
};