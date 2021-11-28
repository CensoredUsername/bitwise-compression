
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::io::{Read, Write};
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};

/// something that can be composed/decomposed into a pile of bits.
pub trait PileOfBits : Default + Copy {
    const BITS: u8;

    fn get_bit(&self, bit: u8) -> bool;

    fn set_bit(&self, bit: u8, value: bool) -> Self;
}

impl PileOfBits for u32 {
    const BITS: u8 = 32;

    fn get_bit(&self, bit: u8) -> bool {
        self & (1 << bit) != 0
    }

    fn set_bit(&self, bit: u8, value: bool) -> Self {
        if value {
            self | (1 << bit)
        } else {
            self & !(1 << bit)
        }
    }
}

impl PileOfBits for u64 {
    const BITS: u8 = 64;

    fn get_bit(&self, bit: u8) -> bool {
        self & (1 << bit) != 0
    }

    fn set_bit(&self, bit: u8, value: bool) -> Self {
        if value {
            self | (1 << bit)
        } else {
            self & !(1 << bit)
        }
    }
}


/// Predictory models models
trait Predictor : std::fmt::Debug {
    /// return minimum range this predictor needs
    fn minrange(&self) -> u32;
    /// predict the chance of the next item being zero. This chance should be at least 1, and maximally the output of `minrange`.
    fn predict(&self, range: u32) -> u32;
    /// feed the predictor a value
    fn feed(&mut self, bit: bool);
}

#[derive(Debug, Clone)]
struct SimplePredictor {
    accumulator: u128,
}

impl SimplePredictor {
    fn new() -> SimplePredictor {
        SimplePredictor {
            accumulator: 0x5555_5555_5555_5555_5555_5555_5555_5555, // 0101010.......1010101
        }
    }
}

impl Predictor for SimplePredictor {
    fn minrange(&self) -> u32 {
        127
    }
    fn predict(&self, range: u32) -> u32 {
        // accumulator can go between +64 and -64 zeros from the average. But a chance of 0 or 1 cannot
        // be encoded with finite precision so we clamp to 1 .. 127
        let zeros = self.accumulator.count_zeros().max(1).min(127);
        (((zeros as u64) * (range as u64)) / 128) as u32
    }
    fn feed(&mut self, bit: bool) {
        self.accumulator <<= 1;
        self.accumulator |= bit as u128;
    }
}


/// Predictor-using binary arithmetic coders

// arithmetic coder that shifts out full bytes at a time.
#[derive(Debug, Clone)]
struct ChunkedArithmeticCoder<P: Predictor> {
    // predictor that tells us the chance of a bit being a 0
    predictor: P,
    // contains the least significant digits of the infinite precision fraction we're currently working on.
    bottom: u32,
    // contains the current range that the fraction is confined too, in the same precision as bottom.
    // effectively, the fraction will always end up being between [bottom.000000, (bottom + range).FFFFFF...]
    range: u32,
}

impl<P: Predictor> ChunkedArithmeticCoder<P> {
    fn new(predictor: P) -> ChunkedArithmeticCoder<P> {
        ChunkedArithmeticCoder {
            predictor,
            bottom: 0,
            range: 0xFFFF_FFFF
        }
    }

    fn encode(&mut self, bitstream: &mut VecDeque<u8>, bit: bool) {
        // if range drops too low we start encoding inefficiently.
        loop {
            if self.bottom >> 24 == (self.range + self.bottom) >> 24 {
                // we can shift a byte out without issues.
            } else if self.range < self.predictor.minrange() {
                // println!("Renormalizing");
                // renormalization is probably a good idea at this point
                let maxrange = 0xFF_FFFF - (self.bottom & 0xFF_FFFF);
                if maxrange < self.range {
                    self.range = maxrange;
                }
            } else {
                break;
            }
            // shift data out and adjust resolution.
            bitstream.push_back((self.bottom >> 24) as u8);
            self.range = (self.range << 8) | 0xFF;
            self.bottom = self.bottom << 8;
        }

        let prob_zero = self.predictor.predict(self.range);
        // assert!(prob_zero != 0);
        // assert!(prob_zero <= self.range);

        self.predictor.feed(bit);
        if bit {
            self.bottom += prob_zero;
            self.range -= prob_zero; 
        } else {
            self.range = prob_zero - 1;
        }
    }

    fn flush(&mut self, bitstream: &mut VecDeque<u8>) {
        self.bottom += self.range / 2;
        for _ in 0 .. 4 {
            bitstream.push_back((self.bottom >> 24) as u8);
            self.bottom <<= 8;
        }
        self.range = 0xFFFF_FFFF;
    }
}


#[derive(Debug, Clone)]
struct ChunkedArithmeticDecoder<P: Predictor> {
    predictor: P,
    bottom: u32,
    range: u32,
    workbuf: u32
}

impl<P: Predictor> ChunkedArithmeticDecoder<P> {
    fn new(predictor: P) -> ChunkedArithmeticDecoder<P> {
        ChunkedArithmeticDecoder {
            predictor,
            bottom: 0,
            range: 0, // by initializing range to zero, the initial shifting loop will prefill workbuf
            workbuf: 0,
        }
    }

    fn decode(&mut self, bitstream: &mut VecDeque<u8>) -> bool {
        // if range drops too low we start encoding inefficiently.
        loop {
            if self.bottom >> 24 == (self.range + self.bottom) >> 24 {
                // we can shift in a byte
            } else if self.range < self.predictor.minrange() {
                // renormalization is probably a good idea at this point
                let maxrange = 0xFF_FFFF - (self.bottom & 0xFF_FFFF);
                if maxrange < self.range {
                    self.range = maxrange;
                }
            } else {
                break;
            }
            // shift new data in and adjust resolution
            self.workbuf = (self.workbuf << 8) | (bitstream.pop_front().unwrap() as u32);
            self.range = (self.range << 8) | 0xFF;
            self.bottom = self.bottom << 8;
        }
        
        let prob_zero = self.predictor.predict(self.range);
        // assert!(prob_zero != 0);
        // assert!(prob_zero <= self.range);

        let bit = self.workbuf >= (self.bottom + prob_zero);

        self.predictor.feed(bit);
        if bit {
            self.bottom += prob_zero;
            self.range -= prob_zero; 
        } else {
            self.range = prob_zero - 1;
        }

        bit
    }
}


pub struct ParallelEncoder<T: PileOfBits> {
    encoders: Vec<ChunkedArithmeticCoder<SimplePredictor>>,
    streams: Vec<VecDeque<u8>>,
    marker: PhantomData<T>,
}

impl<T: PileOfBits> ParallelEncoder<T> {
    pub fn new() -> ParallelEncoder<T> {
        ParallelEncoder {
            encoders: vec![ChunkedArithmeticCoder::new(SimplePredictor::new()); T::BITS as usize],
            streams: vec![VecDeque::new(); T::BITS as usize],
            marker: PhantomData
        }
    }

    pub fn feed(&mut self, value: &T) {
        for i in 0 .. T::BITS {
            self.encoders[i as usize].encode(&mut self.streams[i as usize], value.get_bit(i));
        }
    }

    pub fn flush(&mut self) {
        for i in 0 .. T::BITS {
            self.encoders[i as usize].flush(&mut self.streams[i as usize]);
        }
    }

    pub fn serialize<W: Write>(&mut self, output: &mut W) {
        // need to write amount of streams, length of each stream, and data
        output.write(b"asdf").unwrap();
        output.write_u32::<LittleEndian>(self.encoders.len() as u32).unwrap();
        for stream in &self.streams {
            output.write_u64::<LittleEndian>(stream.len() as u64).unwrap();
        }
        for stream in &mut self.streams {
            output.write(stream.make_contiguous()).unwrap();
        }
    }
}


pub struct ParallelDecoder<T: PileOfBits> {
    decoders: Vec<ChunkedArithmeticDecoder<SimplePredictor>>,
    streams: Vec<VecDeque<u8>>,
    marker: PhantomData<T>,
}

impl<T: PileOfBits> ParallelDecoder<T> {
    pub fn new(streams: Vec<VecDeque<u8>>) -> Result<ParallelDecoder<T>, &'static str> {
        if streams.len() != T::BITS as usize {
            return Err("Bad stream count");
        }
        Ok(ParallelDecoder {
            decoders: vec![ChunkedArithmeticDecoder::new(SimplePredictor::new()); T::BITS as usize],
            streams,
            marker: PhantomData
        })
    }

    pub fn deserialize<R: Read>(input: &mut R) -> Result<ParallelDecoder<T>, &'static str> {
        let mut buf: [u8; 4] = [0; 4];
        input.read_exact(&mut buf).unwrap();
        if b"asdf" != &buf {
            return Err("Bad header");
        }

        let stream_count = input.read_u32::<LittleEndian>().unwrap();
        if stream_count != T::BITS as u32 {
            return Err("Bad stream count");
        }

        let mut stream_lengths = Vec::new();
        for _ in 0 .. T::BITS {
            stream_lengths.push(input.read_u64::<LittleEndian>().unwrap().try_into().map_err(|_| "stream too long")?);
        }

        let mut streams = Vec::new();
        for length in stream_lengths {
            let mut buf = vec![0; length];
            input.read_exact(&mut buf).unwrap();
            streams.push(buf.into());
        }

        Self::new(streams)
    }

    pub fn get(&mut self) -> T {
        let mut t = T::default();
        for i in 0 .. T::BITS {
            t = t.set_bit(i, self.decoders[i as usize].decode(&mut self.streams[i as usize]));
        }
        t
    }
}

#[test]
fn test_troundtrip() {
    // 1000 random numbers in the range 0, 999999
    let test: Vec<u32> = vec![
        707325, 639727, 685002, 891780, 288862, 74782, 969919, 504043, 669354, 940357, 638310, 531450,320098, 489921, 218072, 442620, 588503,
        320585, 675942, 898159, 991356, 656422, 828916, 660335, 291314, 327726, 156641, 208371, 486313, 980500, 56287, 560545, 625505, 601622,
        714519, 511871, 625913, 168861, 221491, 462127, 519426, 484659, 272588, 756430, 293548, 442707, 784319, 388540, 950173, 493920, 430314,
        901165, 893014, 476697, 104515, 50043, 444320, 118689, 455023, 440823, 964561, 513921, 98031, 547874, 766738, 400029, 44941, 825194,
        701046, 547757, 149146, 249748, 543345, 335326, 243733, 269751, 864451, 786761, 865565, 88273, 181442, 626636, 42081, 323136, 563087,
        271216, 845287, 267132, 997605, 50979, 977364, 729701, 165552, 360346, 248607, 639583, 245056, 715249, 976260, 521037, 192468, 749216,
        502938, 265634, 168430, 356057, 627042, 318715, 117230, 983183, 747214, 109606, 38011, 123306, 794031, 409583, 3467, 553029, 432617,
        937989, 718145, 783889, 58440, 9068, 999256, 412936, 324021, 640871, 526369, 132290, 530145, 646937, 250265, 51997, 214267, 990648,
        512717, 347882, 565393, 255381, 722161, 383727, 376220, 791956, 286901, 714070, 519285, 299524, 433203, 727644, 333246, 757806, 352633,
        659097, 701014, 947332, 410141, 22835, 111677, 107870, 149062, 830994, 7792, 672764, 972838, 134485, 534640, 450220, 84998, 977274,
        78859, 213038, 569563, 957985, 805336, 501373, 355750, 469338, 979315, 582447, 191548, 211109, 153800, 83302, 62542, 202452, 42659,
        353718, 71789, 943233, 880026, 482000, 773364, 978159, 860662, 125653, 783768, 657118, 290493, 997868, 292733, 327572, 718400, 773166,
        520897, 191860, 369114, 388688, 820821, 276518, 316638, 627837, 273050, 174522, 695534, 726916, 744341, 245695, 154301, 397487, 398313,
        360252, 629907, 680280, 726311, 832500, 349134, 91744, 547614, 950918, 263717, 315695, 560259, 471301, 760340, 494255, 41934, 546104,
        25420, 43085, 12828, 156524, 970475, 583557, 578564, 228854, 717830, 379426, 224780, 296578, 862172, 914514, 68794, 438397, 595378,
        763830, 584766, 958540, 162191, 817452, 112908, 867036, 310428, 656215, 555526, 502245, 594267, 561968, 930515, 229228, 256366, 155381,
        153218, 531397, 216882, 228626, 793270, 738717, 171255, 471449, 559345, 498861, 396925, 584102, 589426, 784930, 28973, 71582, 108021,
        706625, 369273, 58351, 941314, 396203, 459672, 306204, 525895, 834215, 652694, 518067, 519953, 769451, 320821, 524910, 410927, 441146,
        968312, 516037, 230515, 208139, 898384, 834277, 36479, 795787, 788410, 546258, 540931, 628506, 9041, 503698, 300605, 954679, 358076,
        340483, 33132, 219972, 148339, 947742, 117313, 463011, 164562, 835905, 531918, 194381, 622451, 262740, 152957, 268008, 156757, 990296,
        540820, 239406, 334945, 254918, 566330, 625411, 493758, 870147, 151790, 456944, 589751, 531058, 184343, 817360, 156207, 522047, 159424,
        779356, 204710, 794387, 561505, 148689, 562183, 214069, 642010, 202203, 129725, 299151, 93083, 819807, 218762, 469552, 401895, 931175,
        412592, 679152, 162261, 184252, 638438, 941231, 626965, 282853, 54363, 263483, 506902, 910981, 65860, 397374, 386020, 770129, 160724,
        959831, 214557, 192460, 573686, 54281, 138427, 154276, 474867, 913201, 462861, 121577, 865578, 580551, 670949, 8455, 639251, 957271,
        905224, 555477, 755488, 445379, 821642, 768988, 575795, 404610, 379797, 562562, 454206, 821180, 419414, 249447, 517943, 6519, 827780,
        827693, 984017, 598707, 926386, 510409, 472037, 894993, 441226, 558119, 41826, 544133, 147030, 711061, 470237, 948979, 100223, 533837,
        178978, 908273, 298601, 352865, 262219, 576134, 426632, 971738, 963266, 147934, 618939, 197783, 731404, 134849, 706811, 13073, 426024,
        435171, 101477, 813817, 402787, 850508, 652864, 73365, 131743, 574495, 677844, 743420, 378325, 388793, 969933, 282240, 957520, 622383,
        313498, 738502, 561925, 231442, 292881, 766870, 161213, 82002, 342054, 886358, 865382, 974586, 251475, 894983, 756222, 237615, 467154,
        834783, 879096, 992184, 804414, 312707, 35935, 520467, 153983, 349943, 864345, 488620, 193314, 192073, 551479, 312013, 609667, 117372,
        773301, 828711, 27054, 212953, 274489, 103460, 302127, 815439, 33785, 387196, 662077, 366187, 129374, 520748, 645152, 249822, 354332,
        460086, 298525, 522990, 680747, 9621, 647142, 378794, 904691, 687354, 85440, 706722, 17073, 779021, 227187, 532298, 421647, 63392, 91082,
        670585, 946902, 419148, 694104, 323538, 124777, 96932, 52318, 760943, 751030, 368799, 4395, 545008, 456933, 606308, 982697, 154229,
        617123, 518539, 507021, 199033, 101668, 748256, 245618, 514284, 72571, 973656, 516092, 910434, 42651, 498558, 985907, 596712, 694689,
        100112, 75635, 697632, 579824, 594898, 623069, 526849, 536734, 839400, 247047, 602114, 543437, 15848, 515378, 978375, 315364, 466152,
        858131, 15738, 615164, 28145, 435411, 523238, 511387, 862682, 437141, 445551, 569668, 240116, 194585, 388746, 382756, 377672, 821311,
        476980, 124377, 288800, 866211, 110891, 387255, 554749, 296425, 389034, 705301, 351015, 809785, 113872, 730391, 440094, 480250, 125649,
        868510, 806288, 586048, 42540, 740734, 858599, 168799, 38763, 159392, 241866, 580822, 867163, 887285, 825064, 309513, 667285, 679127,
        514775, 696687, 606818, 933621, 51424, 318735, 516964, 937975, 10742, 242178, 198607, 756360, 802369, 349270, 594033, 380881, 342680,
        370729, 504757, 522250, 976999, 394461, 812048, 557206, 643508, 121785, 84407, 80784, 194206, 911124, 201715, 212482, 949781, 177807,
        428371, 872147, 60762, 897471, 448597, 252231, 695740, 266506, 526611, 131168, 143118, 500195, 357873, 255732, 947404, 937100, 747336,
        204934, 517997, 844612, 44857, 889726, 631571, 855375, 219451, 644563, 743424, 401546, 324351, 44441, 127015, 374387, 84629, 243051,
        992410, 286084, 600983, 219233, 504661, 8212, 58926, 771365, 47260, 37313, 326197, 709713, 37509, 311731, 985524, 340871, 699162, 995492,
        882591, 90543, 323888, 736524, 809487, 853696, 918829, 78268, 955760, 404738, 422176, 536107, 187241, 261040, 974570, 719362, 535959,
        65964, 231357, 528941, 501394, 997660, 124084, 886000, 511006, 563764, 188836, 837825, 292972, 728115, 909839, 901477, 524146, 583550,
        864577, 44783, 40111, 672642, 141336, 444788, 694667, 256897, 980861, 637992, 759831, 264586, 785438, 994988, 349713, 68599, 53713,
        248973, 73704, 842512, 883052, 298912, 292573, 881410, 774208, 142356, 320244, 11496, 887349, 948412, 463495, 471904, 336687, 300751,
        696870, 278418, 329660, 900556, 109593, 2231, 86213, 85945, 896325, 120829, 702845, 116601, 227238, 547502, 288341, 680173, 381823,
        451653, 555073, 625379, 679061, 431708, 489755, 951052, 136748, 11858, 259738, 443486, 394755, 659604, 117413, 254063, 691810, 169196,
        989133, 407382, 145244, 430209, 829852, 99400, 766544, 864179, 393324, 840556, 454572, 118061, 123672, 482678, 798258, 65398, 475024,
        804718, 11579, 52195, 895278, 787060, 4040, 229680, 569478, 916214, 601342, 158538, 690116, 464294, 949935, 5313, 168490, 61511, 944458,
        306539, 457086, 573986, 566171, 103086, 568846, 594465, 804492, 840393, 644633, 457894, 272820, 690827, 48905, 68249, 439505, 118222,
        31566, 59440, 628623, 629590, 78656, 252614, 655136, 407761, 284816, 20512, 455660, 72556, 741068, 287284, 41417, 815583, 277527, 353758,
        83541, 491170, 351584, 535326, 332099, 30503, 186320, 333592, 230593, 682389, 564793, 965802, 958340, 342880, 424593, 334985, 493409,
        590960, 914042, 11024, 587282, 22305, 164398, 580499, 673891, 935288, 205202, 290927, 931264, 407237, 161212, 671502, 739318, 910780,
        687359, 383642, 123141, 922582, 326726, 910766, 521589, 548938, 704946, 595149, 488626, 155474, 13615, 246316, 135022, 562171, 672794,
        132950, 627478, 24235, 595105, 939543, 31092, 731584, 839562, 972199, 745578, 819441, 910810, 533822, 279876, 37639, 32445, 295270,
        867753, 675773, 885787, 70599, 772678, 775329, 490183, 814275, 251585, 900572, 139910, 736519, 589279, 922351, 678663, 586867, 636331,
        787243, 160484, 111770, 486209, 309533, 398657, 766591, 607812, 477886, 220546
    ];

    let mut encoder = ParallelEncoder::<u32>::new();
    for number in &test {
        encoder.feed(number);
    }
    encoder.flush();

    let mut buf = Vec::new();
    encoder.serialize(&mut buf);

    let mut output = Vec::new();
    let mut decoder = ParallelDecoder::<u32>::deserialize(&mut buf.as_slice()).unwrap();

    for _ in 0 .. test.len() {
        output.push(decoder.get());
    }

    let mut cursor = buf.as_slice();
    for _ in 0 .. 33 {
        dbg!(cursor.read_u64::<LittleEndian>().unwrap());
    }

    dbg!(buf.len(), test.len() * std::mem::size_of::<u32>());
    
    assert_eq!(test,output);

    let mut f = std::fs::File::create("test.compressed").unwrap();
    f.write(&buf).unwrap();
}

