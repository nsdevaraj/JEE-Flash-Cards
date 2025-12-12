export interface QuizOption {
  id: string;
  text: string;
}

export interface QuizQuestion {
  id: number;
  question: string;
  options: QuizOption[];
  answer: string; // usually the correct option text or id
  solution?: string; // Explanation often comes as 'solution' or 'explanation' in datasets
  subject?: string; // e.g., 'Physics', 'Chemistry', 'Mathematics'
}

export type FetchStatus = 'idle' | 'loading' | 'success' | 'error';