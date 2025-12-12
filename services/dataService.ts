import { QuizQuestion, QuizOption } from '../types';

const DATA_URL = './quiz.json';
const STORAGE_KEY = 'jee_flashcards_cache_v2'; // Bumped version for new data structure

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
    'phy': 'Physics',
    'chem': 'Chemistry',
    'math': 'Mathematics'
  };
  return map[shortSubject?.toLowerCase()] || shortSubject || 'General';
};

/**
 * Normalizes JEE Question Bank JSON data into our strict QuizQuestion format.
 */
const normalizeData = (data: any): QuizQuestion[] => {
  // The JEE json has a root "questions" array
  const list = Array.isArray(data.questions) ? data.questions : (Array.isArray(data) ? data : []);

  return list.map((item: any, index: number): QuizQuestion | null => {
    if (!item || typeof item !== 'object') return null;

    // Use 'gold' as answer, 'index' as ID
    const qText = item.question || "Question Text Missing";
    const answer = item.gold || item.answer || "";
    const subject = mapSubject(item.subject);
    
    // In this dataset, options are often embedded in the question text or not provided separately.
    // We will keep options empty and let the Question renderer handle the full text.
    const options: QuizOption[] = [];

    // Ensure ID is a number
    const idVal = Number(item.index);
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
    const response = await fetch(DATA_URL);
    if (!response.ok) {
        throw new Error(`Network response was not ok: ${response.status}`);
    }
    
    const rawData = await response.json();
    const normalizedData = normalizeData(rawData);
    
    if (normalizedData.length === 0) {
         throw new Error("Parsed data resulted in 0 valid questions");
    }

    console.log(`Loaded ${normalizedData.length} questions from local JSON.`);
    return normalizedData;

  } catch (error) {
    console.warn("Fetch failed, attempting to load from cache or fallback...", error);
    return FALLBACK_DATA;
  }
};