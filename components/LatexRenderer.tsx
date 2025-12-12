import React, { useMemo } from 'react';
import katex from 'katex';
import { marked } from 'marked';

interface LatexRendererProps {
  text: string;
  className?: string;
  block?: boolean;
}

const LatexRenderer: React.FC<LatexRendererProps> = ({ text, className = "", block = false }) => {
  
  const htmlContent = useMemo(() => {
    if (!text) return "";

    const latexMap = new Map<string, { type: 'block' | 'inline', math: string }>();
    let counter = 0;

    // Helper to replace math with a safe token and store it
    const replaceMath = (str: string, regex: RegExp, type: 'block' | 'inline') => {
      return str.replace(regex, (match, math) => {
        // We use a token that is safe for Markdown (alphanumeric, no special chars like _ or *)
        // "MATHITEM" + index + "END" is safe and unique enough.
        const id = `MATHITEM${counter++}END`;
        latexMap.set(id, { type, math });
        return id;
      });
    };

    let protectedText = text;

    // 1. Protect Block Math $$...$$
    protectedText = replaceMath(protectedText, /\$\$([\s\S]*?)\$\$/g, 'block');
    
    // 2. Protect Block Math \[...\]
    // Note: In JS regex, \[ matches a literal [. We need to match the string \[ ... \]
    protectedText = replaceMath(protectedText, /\\\[([\s\S]*?)\\\]/g, 'block');

    // 3. Protect Inline Math \(...\)
    protectedText = replaceMath(protectedText, /\\\(([\s\S]*?)\\\)/g, 'inline');

    // 4. Protect Inline Math $...$
    // We match $...$ but avoid matching inside already protected tokens (which don't have $)
    protectedText = replaceMath(protectedText, /\$([\s\S]*?)\$/g, 'inline');

    // 5. Parse Markdown
    // Enable breaks to treat \n as <br>
    marked.setOptions({
        gfm: true,
        breaks: true, 
    });
    
    let rawHtml = marked.parse(protectedText) as string;

    // 6. Restore and Render LaTeX
    latexMap.forEach((data, id) => {
      try {
        const rendered = katex.renderToString(data.math, {
          displayMode: data.type === 'block',
          throwOnError: false,
          output: 'html',
          trust: true
        });
        // Replace the token with the rendered HTML. 
        // Using split/join is safer than replace() for tokens that might be repeated or contain special chars (though ours don't).
        rawHtml = rawHtml.split(id).join(rendered);
      } catch (e) {
        console.error("KaTeX Error:", e);
        // Fallback to raw text if render fails
        rawHtml = rawHtml.split(id).join(`<span class="text-red-400 error" title="${e}">$${data.math}$</span>`);
      }
    });

    return rawHtml;
  }, [text]);

  return (
    <div 
      className={`markdown-content ${className}`}
      dangerouslySetInnerHTML={{ __html: htmlContent }} 
    />
  );
};

export default LatexRenderer;